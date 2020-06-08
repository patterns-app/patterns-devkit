from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, Type

from sqlalchemy import Column, DateTime, String

from basis.core.component import ComponentType, ComponentUri
from basis.core.data_function import DataFunctionInterface, DataInterfaceType
from basis.core.data_function_interface import DataFunctionAnnotation
from basis.core.metadata.orm import BaseModel
from basis.core.module import BasisModule
from basis.core.runnable import DataFunctionContext, ExecutionContext
from basis.core.typing.object_type import ObjectTypeLike
from basis.utils.typing import T

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExternalResource(ComponentUri):
    provider: ExternalProvider
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
        self,
        name,
        configured_provider: ConfiguredExternalProvider = None,
        initial_high_water_mark: datetime = None,
        **kwargs,
    ) -> ConfiguredExternalResource:
        # TODO: would be AWEsome to have a proper signature on this method (read dynamically from configuration class?
        # TODO: also want to auto configure Source as well here, and maybe pluck out relevant kwargs, as a shortcut?
        cfg = None
        args = (self.initial_configuration or {}).copy()
        args.update(**kwargs)
        if self.configuration_class:
            cfg = self.configuration_class(**args)
        if not initial_high_water_mark:
            initial_high_water_mark = self.initial_high_water_mark
        if not configured_provider:
            configured_provider = self.provider(name + "_provider")
        return ConfiguredExternalResource(
            name=name,
            external_resource=self,
            configuration=cfg,
            configured_provider=configured_provider,
            initial_high_water_mark=initial_high_water_mark,
        )


def external_resource_factory(**kwargs: Any) -> ExternalResource:
    kwargs["component_type"] = ComponentType.External
    kwargs["module_name"] = kwargs.get("module_name")
    kwargs["version"] = kwargs.get("version")
    return ExternalResource(**kwargs)


ExternalDataResource = external_resource_factory


@dataclass(frozen=True)
class ConfiguredExternalResource(Generic[T]):
    name: str
    external_resource: ExternalResource
    configured_provider: ConfiguredExternalProvider
    configuration: Optional[T] = None
    initial_high_water_mark: Optional[datetime] = None
    # Data set expected size ? 1e2, country = 1e2, EcommOrder = 1e6 (Optional for sure, and overridable as "compiler hint")

    def __getattr__(self, item):
        return getattr(self.external_resource, item)

    def get_config_value(self, key: str, default: Any = None) -> Any:
        return getattr(self.configuration, key, default)

    def get_state(self, ctx: ExecutionContext) -> ConfiguredExternalResourceState:
        state = (
            ctx.metadata_session.query(ConfiguredExternalResourceState)
            .filter(
                ConfiguredExternalResourceState.configured_external_resource_name
                == self.name
            )
            .first()
        )
        if state is None:
            state = ConfiguredExternalResourceState(
                configured_external_resource_name=self.name
            )
            state = ctx.add(state)
        return state

    def reset(self, ctx: ExecutionContext):
        state = self.get_state(ctx)
        state.high_water_mark = None  # type: ignore  # sqlalchemy

    @property
    def extractor(self) -> ExtractorDataFunction:
        return ExtractorDataFunction(
            self.external_resource.default_extractor, self.configured_provider, self
        )


class ConfiguredExternalResourceState(BaseModel):
    configured_external_resource_name = Column(String, primary_key=True)
    high_water_mark = Column(DateTime, nullable=True)

    def __repr__(self):
        return self._repr(
            configured_external_resource_name=self.configured_external_resource_name,
            high_water_mark=self.high_water_mark,
        )

    def set_high_water_mark(self, hwm: datetime):
        print(hwm)
        self.high_water_mark = hwm


class ExternalResourceList(list):
    # We subclass list so we can add resources directly as attributes to the list
    pass


@dataclass(frozen=True)
class ExternalProvider(ComponentUri):
    name: str
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

    def __call__(self, name, **kwargs) -> ConfiguredExternalProvider:
        cfg = None
        args = (self.initial_configuration or {}).copy()
        args.update(**kwargs)
        if self.configuration_class:
            cfg = self.configuration_class(**args)
        return ConfiguredExternalProvider(name=name, provider=self, configuration=cfg)

    # def get_resources(self) -> Sequence[SourceResource]:
    #     return self._resources

    def add_resource(self, external_resource: ExternalResource):
        for r in self.resources:
            if r.name == external_resource.name:
                # Don't add twice
                return
        self.resources.append(external_resource)
        setattr(self.resources, external_resource.name, external_resource)

    def get_resource(self, resource_name: str) -> ExternalResource:
        for r in self.resources:
            if r.name == resource_name:
                return r
        raise Exception(f"No resource {resource_name}")

    # TODO: too much magic?
    def __getattr__(self, item) -> ExternalResource:
        try:
            return self.get_resource(item)
        except Exception as e:
            raise AttributeError(e)

    def __dir__(self) -> List[str]:
        d = super().__dir__()
        return list(set(d) | set([r.name for r in self.resources]))

    def associate_with_module(self, module: BasisModule) -> ComponentUri:
        resources: ExternalResourceList = ExternalResourceList()
        for r in self.resources:
            resources.append(r.associate_with_module(module))
        return self.clone(module_name=module.name, resources=resources)


def external_provider_factory(**kwargs: Any) -> ExternalProvider:
    kwargs["component_type"] = ComponentType.External
    kwargs["module_name"] = kwargs.get("module_name")
    kwargs["version"] = kwargs.get("version")
    return ExternalProvider(**kwargs)


# TODO: not a fan of this. How do we provide proper init on dataclasses?
#   Let them be mutable? "Final" hint would be ideal...
DataProvider = external_provider_factory


@dataclass(frozen=True)
class ConfiguredExternalProvider(Generic[T]):
    name: str
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


class ExtractorDataFunction:
    def __init__(
        self,
        extractor_function: ExtractorLike,
        configured_provider: ConfiguredExternalProvider,
        configured_external_resource: ConfiguredExternalResource,
        name: str = None,
    ):
        # super().__init__(extractor_function, name)
        self.extract_function = extractor_function
        self.configured_provider = configured_provider
        self.configured_external_resource = configured_external_resource

    def prepare_state(
        self, state: ConfiguredExternalResourceState,
    ):
        if state.high_water_mark is None:
            state.high_water_mark = (
                self.configured_external_resource.initial_high_water_mark
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

    def __call__(
        self, *args: DataFunctionContext, **kwargs: DataInterfaceType
    ) -> DataInterfaceType:
        ctx = args[0]
        state = self.configured_external_resource.get_state(ctx.execution_context)
        self.prepare_state(state)
        for extract_result in self.extract_function(
            self.configured_provider, self.configured_external_resource, state,
        ):
            if extract_result.records is not None:
                yield extract_result.records
            self.set_state(state, extract_result)

    def get_interface(self) -> DataFunctionInterface:
        # TODO: more than dictlistiterator
        #   get it from actual extractor!
        out_annotation = DataFunctionAnnotation.create(
            data_format_class="DictListIterator",
            otype_like=self.configured_external_resource.otype,
        )
        return DataFunctionInterface(
            inputs=[], output=out_annotation, requires_data_function_context=True
        )


# def extractor_decorator(extract_function: ExtractFunction):
#     raise
#
#     def extractor_factory(
#         configured_provider: ConfiguredSource,
#         configured_external_resource: ConfiguredSourceResource,
#         key: str = None,
#     ):
#         return ExtractDataFunction(
#             extract_function, configured_provider, configured_external_resource, key=key
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
#     def csr_key_from_node(node: ConfiguredDataFunction) -> str:
#         return f"_mock_source_resource_from_node_{node.key}"
#
#     @wraps(df)
#     def static_source(*args, **kwargs):
#         ctx: Any = args[0]  # DataFunctionContext = args[0]
#         key = csr_key_from_node(ctx.node)
#         state = (
#             ctx._metadata_session.query(ConfiguredSourceResourceState)
#             .filter(ConfiguredSourceResourceState.configured_external_resource_key == key)
#             .first()
#         )
#         if state is not None:
#             # Already run once, don't run again
#             raise InputExhaustedException()
#         ret = df(*args, **kwargs)
#         state = ConfiguredSourceResourceState(
#             configured_external_resource_key=key, high_water_mark=utcnow(),
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
#         configured_provider: ConfiguredSource,
#         configured_external_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> str:
#         raise NotImplementedError
#
#     def get_params_from_configuration(
#         self,
#         configured_provider: ConfiguredSource,
#         configured_external_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Dict:
#         raise NotImplementedError
#
#     def __call__(
#         self,
#         configured_provider: ConfiguredSource,
#         configured_external_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Iterator[ExtractResult]:
#         params = self.get_params_from_configuration(
#             configured_provider, configured_external_resource, state
#         )
#         url = self.get_url_from_configuration(
#             configured_provider, configured_external_resource, state
#         )
#         for extract_result in self.iterate_extract_results(
#             url, params, configured_provider, configured_external_resource, state
#         ):
#             yield extract_result
#
#     def iterate_extract_results(
#         self,
#         url: str,
#         params: Dict,
#         configured_provider: ConfiguredSource,
#         configured_external_resource: ConfiguredSourceResource,
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
#         configured_provider: ConfiguredSource,
#         configured_external_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> str:
#         return self.default_path
#
#     def get_params_from_configuration(
#         self,
#         configured_provider: ConfiguredSource,
#         configured_external_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Dict:
#         return {}
#
#     def __call__(
#         self,
#         configured_provider: ConfiguredSource,
#         configured_external_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Iterator[ExtractResult]:
#         params = self.get_params_from_configuration(
#             configured_provider, configured_external_resource, state
#         )
#         path = self.get_path_from_configuration(
#             configured_provider, configured_external_resource, state
#         )
#         for extract_result in self.get_extract_results(
#             path, params, configured_provider, configured_external_resource, state
#         ):
#             yield extract_result
#
#     def get_extract_results(
#         self,
#         path: str,
#         params: Dict,
#         configured_provider: ConfiguredSource,
#         configured_external_resource: ConfiguredSourceResource,
#         state: ConfiguredSourceResourceState,
#     ) -> Iterator[ExtractResult]:
#         raise NotImplementedError
