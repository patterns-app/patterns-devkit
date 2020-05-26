from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, Type, Union

import requests
from ratelimit import limits, sleep_and_retry
from sqlalchemy import Column, DateTime, String

from basis.core.data_function import (
    DataFunctionInterface,
    DataInterfaceType,
    PythonDataFunction,
    TypedDataAnnotation,
)
from basis.core.metadata.orm import BaseModel
from basis.core.object_type import ObjectTypeLike
from basis.core.runnable import DataFunctionContext, ExecutionContext
from basis.utils.registry import T

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExternalResource:
    provider: ExternalProvider
    key: str
    verbose_name: str
    description: str
    otype: ObjectTypeLike
    default_extractor: ExtractorLike
    # default_loader: LoaderLike  # TODO
    configuration_class: Optional[Type] = None
    initial_configuration: Optional[Dict[str, Any]] = None
    initial_high_water_mark: Optional[datetime] = None
    is_public: bool = False
    # parent: Optional[SourceResource] = None
    # Data set expected size ? 1e2, country = 1e2, EcommOrder = 1e6 (Optional for sure, and overridable as "compiler hint")
    # TODO: importance / ranking / popularity: int ????
    # supported_record_formats: list[MemoryFormat] = field(default_factory=list)
    # default_private: bool = True

    def __post_init__(self):
        self.provider.add_resource(self)

    def __call__(
        self, key, configured_source=None, **kwargs
    ) -> ConfiguredExternalResource:
        # TODO: would be AWEsome to have a proper signature on this method (read dynamically from configuration class?
        # TODO: also want to auto configure Source as well here, and maybe pluck out relevant kwargs, as a shortcut?
        cfg = None
        args = (self.initial_configuration or {}).copy()
        args.update(**kwargs)
        if self.configuration_class:
            cfg = self.configuration_class(**args)
        return ConfiguredExternalResource(
            key=key,
            external_resource=self,
            configuration=cfg,
            configured_provider=configured_source,
        )


@dataclass(frozen=True)
class ConfiguredExternalResource(Generic[T]):
    key: str
    external_resource: ExternalResource
    configured_provider: ConfiguredExternalProvider
    configuration: Optional[T] = None
    # Data set expected size ? 1e2, country = 1e2, EcommOrder = 1e6 (Optional for sure, and overridable as "compiler hint")

    def __getattr__(self, item):
        return getattr(self.external_resource, item)

    def get_config_value(self, key: str, default: Any = None) -> Any:
        return getattr(self.configuration, key, default)

    def get_state(self, ctx: ExecutionContext) -> ConfiguredExternalResourceState:
        state = (
            ctx.metadata_session.query(ConfiguredExternalResourceState)
            .filter(
                ConfiguredExternalResourceState.configured_external_resource_key
                == self.key
            )
            .first()
        )
        if state is None:
            state = ConfiguredExternalResourceState(
                configured_source_resource_key=self.key
            )
            state = ctx.add(state)
        return state

    def reset(self, ctx: ExecutionContext):
        state = self.get_state(ctx)
        state.high_water_mark = None  # type: ignore

    @property
    def extractor(self):
        return ExtractorDataFunction(
            self.external_resource.default_extractor, self.configured_provider, self
        )


class ConfiguredExternalResourceState(BaseModel):
    configured_external_resource_key = Column(String, primary_key=True)
    high_water_mark = Column(DateTime, nullable=True)

    def __repr__(self):
        return self._repr(
            configured_source_resource_key=self.configured_external_resource_key,
            high_water_mark=self.high_water_mark,
        )

    def set_high_water_mark(self, hwm: datetime):
        print(hwm)
        self.high_water_mark = hwm


class ExternalResourceList(list):
    pass


@dataclass(frozen=True)
class ExternalProvider:
    key: str
    verbose_name: str
    description: str
    resources: ExternalResourceList = field(default_factory=ExternalResourceList)
    # authenticators: list[AuthenticatorBaseView]
    requires_authentication: bool = False
    configuration_class: Optional[Type] = None
    initial_configuration: Optional[Dict[str, Any]] = None
    is_public: bool = False
    unregistered: bool = False

    # def get_default_authenticator(self):
    #     return self.authenticators[0]

    def __call__(self, key, **kwargs) -> ConfiguredExternalProvider:
        cfg = None
        args = (self.initial_configuration or {}).copy()
        args.update(**kwargs)
        if self.configuration_class:
            cfg = self.configuration_class(**args)
        return ConfiguredExternalProvider(key=key, provider=self, configuration=cfg)

    # def get_resources(self) -> Sequence[SourceResource]:
    #     return self._resources

    def add_resource(self, external_resource: ExternalResource):
        self.resources.append(external_resource)
        setattr(self.resources, external_resource.key, external_resource)

    def get_resource(self, resource_key: str) -> ExternalResource:
        for r in self.resources:
            if r.key == resource_key:
                return r
        raise Exception(f"No resource {resource_key}")

    # TODO: too much magic?
    def __getattr__(self, item) -> ExternalResource:
        try:
            return self.get_resource(item)
        except Exception as e:
            raise AttributeError(e)

    def __dir__(self) -> List[str]:
        d = super().__dir__()
        return list(set(d) | set([r.key for r in self.resources]))


@dataclass(frozen=True)
class ConfiguredExternalProvider(Generic[T]):
    key: str
    provider: ExternalProvider
    configuration: Optional[T] = None

    def __getattr__(self, item):
        return getattr(self.provider, item)

    def __dir__(self) -> List[str]:
        return self.provider.__dir__()

    def get_config_value(self, key: str, default: Any = None) -> Any:
        return getattr(self.configuration, key, default)


@dataclass(frozen=True)
class ExtractorResult:
    # more_to_extract: bool
    records: Optional[DataInterfaceType] = None  # [DictList]
    new_high_water_mark: Optional[datetime] = None
    new_state: Optional[Dict] = None


ExtractorLike = Callable[
    [
        # Any,  # DataFunctionContext,
        ConfiguredExternalProvider,
        ConfiguredExternalResource,
        ConfiguredExternalResourceState,
    ],
    Iterator[ExtractorResult],
]


class ExtractorDataFunction(PythonDataFunction):
    def __init__(
        self,
        extractor_function: ExtractorLike,
        configured_source: ConfiguredExternalProvider,
        configured_source_resource: ConfiguredExternalResource,
        key: str = None,
    ):
        super().__init__(extractor_function, key)
        self.extract_function = extractor_function
        self.configured_source = configured_source
        self.configured_source_resource = configured_source_resource

    def prepare_state(
        self, state: ConfiguredExternalResourceState,
    ):
        if state.high_water_mark is None:
            state.high_water_mark = (
                self.configured_source_resource.initial_high_water_mark
            )
            # if state.high_water_mark is None and hasattr(
            #     self.extract_function, "initial_high_water_mark"
            # ):
            #     state.high_water_mark = (
            #         self.extract_function.initial_high_water_mark
            #     )  # TODO: don't do this here?
        # TODO: handle arbitrary state blob

    def set_state(
        self, state: ConfiguredExternalResourceState, extract_result: ExtractorResult
    ):
        if extract_result.new_high_water_mark:
            state.set_high_water_mark(extract_result.new_high_water_mark)
        print("Setting state", state.high_water_mark)
        # TODO: handle arbitrary state blob

    def __call__(self, ctx: DataFunctionContext) -> DataInterfaceType:
        state = self.configured_source_resource.get_state(ctx.execution_context)
        self.prepare_state(state)
        for extract_result in self.extract_function(
            self.configured_source, self.configured_source_resource, state,
        ):
            if extract_result.records is not None:
                yield extract_result.records
            self.set_state(state, extract_result)

    def get_interface(self) -> DataFunctionInterface:
        # TODO: more than dictlistiterator
        out_annotation = TypedDataAnnotation.create(
            data_resource_class="DictListIterator",
            otype_like=self.configured_source_resource.otype,
        )
        return DataFunctionInterface(
            inputs=[], output=out_annotation, requires_data_function_context=True
        )


# def extractor_decorator(extract_function: ExtractFunction):
#     raise
#
#     def extractor_factory(
#         configured_source: ConfiguredSource,
#         configured_source_resource: ConfiguredSourceResource,
#         key: str = None,
#     ):
#         return ExtractDataFunction(
#             extract_function, configured_source, configured_source_resource, key=key
#         )
#
#     return extractor_factory


# Decorator
# extractor = extractor_decorator


# TODO: redo this when needed
# def static_source_resource(df: DataFunctionCallable) -> DataFunctionCallable:
#     """
#     Only run function once, since source data is static and only one output
#     """
#
#     def csr_key_from_cdf(cdf: ConfiguredDataFunction) -> str:
#         return f"_mock_source_resource_from_cdf_{cdf.key}"
#
#     @wraps(df)
#     def static_source(*args, **kwargs):
#         ctx: Any = args[0]  # DataFunctionContext = args[0]
#         key = csr_key_from_cdf(ctx.cdf)
#         state = (
#             ctx._metadata_session.query(ConfiguredSourceResourceState)
#             .filter(ConfiguredSourceResourceState.configured_source_resource_key == key)
#             .first()
#         )
#         if state is not None:
#             # Already run once, don't run again
#             raise InputExhaustedException()
#         ret = df(*args, **kwargs)
#         state = ConfiguredSourceResourceState(
#             configured_source_resource_key=key, high_water_mark=utcnow(),
#         )
#         ctx._metadata_session.add(state)
#         return ret
#
#     # TODO: hmmm, we should have a unified interface for all DataFunctions
#     #   Probably has to be class / class wrapper?
#     #   wrapped either with decorator at declare time or ensured at runtime?
#     if hasattr(df, "get_interface"):
#
#         def call_(self, *args, **kwargs):
#             return static_source(*args, **kwargs)
#
#         df.__call__ = call_
#         return df
#
#         # class S:
#         #     def __call__(self, *args, **kwargs):
#         #         return static_source(*args, **kwargs)
#         #
#         #     def __getattr__(self, item):
#         #         return getattr(df, item)
#         #
#         # return S()
#
#     return static_source


# class JsonHttpApiExtractor:
#     initial_high_water_mark: datetime = datetime(1970, 1, 1)
#
#     def get_url_from_configuration(
#         self,
#         configured_source: ConfiguredSource,
#         configured_source_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> str:
#         raise NotImplementedError
#
#     def get_params_from_configuration(
#         self,
#         configured_source: ConfiguredSource,
#         configured_source_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Dict:
#         raise NotImplementedError
#
#     def __call__(
#         self,
#         configured_source: ConfiguredSource,
#         configured_source_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Iterator[ExtractResult]:
#         params = self.get_params_from_configuration(
#             configured_source, configured_source_resource, state
#         )
#         url = self.get_url_from_configuration(
#             configured_source, configured_source_resource, state
#         )
#         for extract_result in self.iterate_extract_results(
#             url, params, configured_source, configured_source_resource, state
#         ):
#             yield extract_result
#
#     def iterate_extract_results(
#         self,
#         url: str,
#         params: Dict,
#         configured_source: ConfiguredSource,
#         configured_source_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Iterator[ExtractResult]:
#         raise NotImplementedError
#
#
# class JsonHttpApiExtractor:
#     base_url: str
#     default_params: Dict = {}
#     date_format: str = "%Y-%m-%d %H:%M:%S"
#     raise_for_status: bool = True
#     ratelimit_calls_per_min: int = 1000
#     initial_high_water_mark: datetime = datetime(1970, 1, 1)
#     default_path: str = ""
#
#     def __init__(self):
#         self.add_rate_limiting()
#
#     def add_rate_limiting(self):
#         g = sleep_and_retry(self.get)
#         g = limits(calls=self.ratelimit_calls_per_min, period=60)(g)
#         self.get = g
#
#     def get_default_params(self):
#         return self.default_params
#
#     def format_params(self, params: Dict) -> Dict:
#         # Must be idempotent!
#         formatted = {}
#         for k, v in params.items():
#             if isinstance(v, datetime) or isinstance(v, date):
#                 v = v.strftime(self.date_format)
#             formatted[k] = v
#         return formatted
#
#     def get_url(self, path: str) -> str:
#         return self.base_url.rstrip() + "/" + path.lstrip("/")
#
#     def get(self, path, params) -> Dict:
#         default_params = self.get_default_params()
#         default_params.update(params)
#         final_params = self.format_params(default_params)
#         url = self.get_url(path)
#         resp = requests.get(url, params=final_params)
#         if self.raise_for_status:
#             resp.raise_for_status()
#         return resp.json()
#
#     def get_path_from_configuration(
#         self,
#         configured_source: ConfiguredSource,
#         configured_source_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> str:
#         return self.default_path
#
#     def get_params_from_configuration(
#         self,
#         configured_source: ConfiguredSource,
#         configured_source_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Dict:
#         return {}
#
#     def __call__(
#         self,
#         configured_source: ConfiguredSource,
#         configured_source_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Iterator[ExtractResult]:
#         params = self.get_params_from_configuration(
#             configured_source, configured_source_resource, state
#         )
#         path = self.get_path_from_configuration(
#             configured_source, configured_source_resource, state
#         )
#         for extract_result in self.get_extract_results(
#             path, params, configured_source, configured_source_resource, state
#         ):
#             yield extract_result
#
#     def get_extract_results(
#         self,
#         path: str,
#         params: Dict,
#         configured_source: ConfiguredSource,
#         configured_source_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Iterator[ExtractResult]:
#         raise NotImplementedError
