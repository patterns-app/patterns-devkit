from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union

from pandas import DataFrame

from basis.core.data_block import DataBlockMetadata, DataSetMetadata
from basis.core.data_format import DatabaseTable, DictList
from basis.core.data_function_interface import DataFunctionInterface
from basis.core.runtime import RuntimeClass
from basis.utils.uri import UriMixin

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
    if "ql" in runtime.lower() or "postgre" in runtime.lower():
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


@dataclass(frozen=True, eq=False)
class DataFunction(UriMixin):
    function_callable: Optional[Callable]
    runtime_class: RuntimeClass
    is_composite: bool
    sub_functions: List[DataFunction] = field(default_factory=list)
    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies

    def __call__(
        self, *args: DataFunctionContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        return self.function_callable(*args, **kwargs)

    def get_interface(self) -> Optional[DataFunctionInterface]:
        if self.function_callable is None:
            assert self.is_composite is not None
            return None
        if hasattr(self.function_callable, "get_interface"):
            return self.function_callable.get_interface()
        return DataFunctionInterface.from_datafunction_definition(
            self.function_callable
        )


def data_function_factory(
    data_function: Optional[DataFunctionCallable],
    key: str = None,
    version: str = None,
    runtime: str = None,
    module_key: str = None,
    is_composite: bool = False,
    **kwargs: Any,
) -> DataFunction:
    if key is None:
        if data_function is None:
            raise
        key = make_datafunction_key(data_function)
    runtime_class = get_runtime_class(runtime)
    return DataFunction(
        key=key,
        module_key=module_key,
        version=version,
        function_callable=data_function,
        runtime_class=runtime_class,
        is_composite=is_composite,
        **kwargs,
    )


def datafunction(
    df_or_key: Union[str, DataFunctionCallable] = None,
    key: str = None,
    version: str = None,
    runtime: str = None,
    module_key: str = None,
) -> Union[Callable, DataFunction]:
    if isinstance(df_or_key, str) or df_or_key is None:
        return partial(
            datafunction,
            key=df_or_key,
            version=version,
            runtime=runtime,
            module_key=module_key,
        )
    return data_function_factory(
        df_or_key, key=key, version=version, runtime=runtime, module_key=module_key
    )


DataFunctionLike = Union[DataFunctionCallable, DataFunction]


def datafunction_chain(
    key: str, function_chain: List[DataFunctionLike], **kwargs
) -> DataFunction:
    sub_funcs = []
    for fn in function_chain:
        df = ensure_datafunction(fn, **kwargs)
        sub_funcs.append(df)
    return data_function_factory(
        None, key=key, sub_functions=sub_funcs, is_composite=True, **kwargs
    )


def ensure_datafunction(dfl: DataFunctionLike, **kwargs) -> DataFunction:
    if isinstance(dfl, DataFunction):
        return dfl
    return data_function_factory(dfl, **kwargs)
