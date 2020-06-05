from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union

from pandas import DataFrame

from basis.core.data_block import DataBlockMetadata, DataSetMetadata
from basis.core.data_format import DatabaseTable, DictList
from basis.core.data_function_interface import DataFunctionInterface
from basis.core.registries import DataFunctionRegistry
from basis.core.runtime import RuntimeClass
from basis.utils.uri import DEFAULT_MODULE_KEY, UriMixin

if TYPE_CHECKING:
    from basis.core.runnable import DataFunctionContext


class DataFunctionException(Exception):
    pass


class InputExhaustedException(DataFunctionException):
    pass


DataFunctionCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame, DictList, DatabaseTable, DataBlockMetadata, DataSetMetadata
]  # TODO: also input...?   Isn't this duplicated with the Interface list AND with DataFormats?


def get_runtime_class(runtime: Optional[str]) -> RuntimeClass:
    if runtime is None:
        return RuntimeClass.PYTHON
    if (
        "ql" in runtime.lower()
        or "database" in runtime.lower()
        or "postgre" in runtime.lower()
    ):
        return RuntimeClass.DATABASE
    return RuntimeClass.PYTHON


def make_datafunction_key(data_function: DataFunctionCallable) -> str:
    # TODO: something more principled / explicit?
    if hasattr(data_function, "key"):
        return data_function.key  # type: ignore
    if hasattr(data_function, "__name__"):
        return data_function.__name__
    if hasattr(data_function, "__class__"):
        return data_function.__class__.__name__
    raise Exception(f"Invalid DataFunction Key {data_function}")


@dataclass
class DataFunction(UriMixin):
    runtime_data_functions: Dict[RuntimeClass, DataFunctionDefinition] = field(
        default_factory=dict
    )
    __hash__ = UriMixin.__hash__

    def add_definition(self, df: DataFunctionDefinition):
        for cls in df.supported_runtime_classes:
            self.runtime_data_functions[cls] = df

    def validate(self):
        # TODO: check if all function signatures match
        pass

    def merge(self, other: DataFunction, overwrite: bool = False):
        for cls, df in other.runtime_data_functions.items():
            if cls not in self.runtime_data_functions:
                self.runtime_data_functions[cls] = df

    def get_definition(
        self, runtime_cls: RuntimeClass
    ) -> Optional[DataFunctionDefinition]:
        return self.runtime_data_functions.get(runtime_cls)


@dataclass
class DataFunctionDefinition(UriMixin):
    function_callable: Optional[
        Callable
    ]  # Optional since Composite DFs don't have a Callable
    supported_runtime_classes: List[RuntimeClass]
    is_composite: bool = False
    configuration_class: Optional[Type] = None
    sub_functions: List[DataFunctionDefinitionLike] = field(
        default_factory=list
    )  # TODO: support proper graphs
    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies

    __hash__ = UriMixin.__hash__

    def __call__(
        self, *args: DataFunctionContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        return self.function_callable(*args, **kwargs)

    def get_interface(self) -> Optional[DataFunctionInterface]:
        if self.function_callable is None:
            assert self.is_composite
            return None
        if hasattr(self.function_callable, "get_interface"):
            return self.function_callable.get_interface()
        return DataFunctionInterface.from_datafunction_definition(
            self.function_callable
        )

    def as_data_function(self) -> DataFunction:
        df = DataFunction(
            key=self.key, module_key=self.module_key, version=self.version
        )
        df.add_definition(self)
        return df


DataFunctionLike = Union[DataFunctionCallable, DataFunctionDefinition, DataFunction]


@dataclass(frozen=True)
class LazyDataFunction:
    uri_or_key: Optional[str] = None
    df_like: Optional[DataFunctionLike] = None
    df_like_kwargs: Optional[Dict] = None

    def resolve(self, registry: DataFunctionRegistry) -> DataFunction:
        if self.uri_or_key:
            return registry.get(self.uri_or_key)
        dfl = self.df_like
        if self.df_like_kwargs:
            dfl = datafunction(**self.df_like_kwargs)
        return registry.process(dfl)


def data_function_definition_factory(
    function_callable: Optional[
        DataFunctionCallable
    ],  # Composite DFs don't have a callable
    key: str = None,
    version: str = None,
    supported_runtimes: str = None,
    module_key: str = None,
    **kwargs: Any,
) -> DataFunctionDefinition:
    if key is None:
        if function_callable is None:
            raise
        key = make_datafunction_key(function_callable)
    runtime_class = get_runtime_class(supported_runtimes)
    if not module_key:
        module_key = DEFAULT_MODULE_KEY
    return DataFunctionDefinition(
        key=key,
        module_key=module_key,
        version=version,
        function_callable=function_callable,
        supported_runtime_classes=[runtime_class],
        **kwargs,
    )


def datafunction(
    df_or_key: Union[str, DataFunctionCallable] = None,
    key: str = None,
    version: str = None,
    supported_runtimes: str = None,
    module_key: str = None,
) -> Union[Callable, DataFunctionDefinition]:
    if isinstance(df_or_key, str) or df_or_key is None:
        return partial(
            datafunction,
            key=df_or_key,
            version=version,
            supported_runtimes=supported_runtimes,
            module_key=module_key,
        )
    return data_function_definition_factory(
        df_or_key,
        key=key,
        version=version,
        supported_runtimes=supported_runtimes,
        module_key=module_key,
    )


def datafunction_chain(
    key: str, function_chain: List[Union[DataFunctionLike, str]], **kwargs
) -> DataFunctionDefinition:
    sub_funcs = []
    for fn in function_chain:
        if isinstance(fn, str):
            df = fn  # is a key, we will lazy resolve it when we have an env
        else:
            df = ensure_datafunction_definition(fn, **kwargs)
        sub_funcs.append(df)
    return data_function_definition_factory(
        None, key=key, sub_functions=sub_funcs, is_composite=True, **kwargs
    )


def ensure_datafunction_definition(
    dfl: DataFunctionLike, **kwargs
) -> DataFunctionDefinition:
    if isinstance(dfl, DataFunctionDefinition):
        return dfl
    return data_function_definition_factory(dfl, **kwargs)


def ensure_datafunction(dfl: DataFunctionLike, **kwargs) -> DataFunction:
    if isinstance(dfl, DataFunction):
        return dfl
    dfd = ensure_datafunction_definition(dfl, **kwargs)
    return dfd.as_data_function()
