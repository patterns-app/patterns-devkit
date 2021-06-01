from __future__ import annotations

import inspect
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from commonmodel.base import SchemaLike
from dcp.data_format.formats.memory.records import Records
from pandas import DataFrame
from snapflow.core.component import (
    DEFAULT_LOCAL_NAMESPACE,
    DEFAULT_NAMESPACE,
    global_library,
)
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.declarative.function import DataFunctionCfg, DataFunctionInterfaceCfg
from snapflow.core.function_interface import (  # merge_declared_interface_with_signature_interface,
    Parameter,
    function_interface_from_callable,
)
from snapflow.core.module import DEFAULT_LOCAL_MODULE, SnapflowModule
from snapflow.core.runtime import DatabaseRuntimeClass, PythonRuntimeClass, RuntimeClass

if TYPE_CHECKING:
    from snapflow.core.execution import DataFunctionContext
    from snapflow import Environment
    from snapflow.core.function_package import DataFunctionPackage


class DataFunctionException(Exception):
    pass


class InputExhaustedException(DataFunctionException):
    pass


DataFunctionCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame,
    Records,
    DataBlockMetadata,
    DataBlock,
]  # TODO: also input...?   Isn't this duplicated with the Interface list AND with DataFormats?


def make_function_name(function: Union[DataFunctionCallable, DataFunction, str]) -> str:
    # TODO: something more principled / explicit?
    if isinstance(function, str):
        return function
    if hasattr(function, "name"):
        return function.name  # type: ignore
    if hasattr(function, "__name__"):
        return function.__name__
    if hasattr(function, "__class__"):
        return function.__class__.__name__
    raise Exception(f"Cannot make name for function-like {function}")


@dataclass
class DataFunction:
    # Underscored so the decorator API can use `DataFunction`. TODO: Is there a better way / name?
    name: str
    namespace: str
    function_callable: Callable
    required_storage_classes: List[str] = field(default_factory=list)
    required_storage_engines: List[str] = field(default_factory=list)
    # compatible_runtime_classes: List[Type[RuntimeClass]]
    # params: List[Parameter] = field(default_factory=list)
    state_class: Optional[Type] = None
    # declared_inputs: Optional[List[DataFunctionInput]] = None
    # declared_output: Optional[DataFunctionOutput] = None
    ignore_signature: bool = (
        False  # Whether to ignore signature if there are any declared i/o
    )
    _original_object: Any = None
    package: DataFunctionPackage = None
    display_name: Optional[str] = None
    description: Optional[str] = None

    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies
    def _post_init(self, *args, **kwargs):
        global_library.add_function(self)

    @property
    def key(self) -> str:
        k = self.name
        if self.namespace:
            k = self.namespace + "." + k
        return k

    def __call__(
        self, *args: DataFunctionContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        return self.function_callable(*args, **kwargs)

    def get_original_object(self) -> Any:
        return self._original_object or self.function_callable

    def get_interface(self) -> DataFunctionInterfaceCfg:
        """"""
        found_signature_interface = self._get_function_interface()
        return found_signature_interface
        # declared_interface = DataFunctionInterface(
        #     inputs=self.declared_inputs or [], output=self.declared_output
        # )
        # return merge_declared_interface_with_signature_interface(
        #     declared_interface,
        #     found_signature_interface,
        #     ignore_signature=self.ignore_signature,
        # )

    def to_config(self) -> DataFunctionCfg:
        return DataFunctionCfg(
            name=self.name,
            namespace=self.namespace,
            interface=self.get_interface(),
            required_storage_classes=self.required_storage_classes,
            required_storage_engines=self.required_storage_engines,
            ignore_signature=self.ignore_signature,
            # TODO: rest
        )

    @property
    def params(self) -> Dict[str, Parameter]:
        return self.get_interface().parameters

    def get_param(self, name: str) -> Parameter:
        return self.get_interface().parameters[name]

    def _get_function_interface(self) -> DataFunctionInterfaceCfg:
        if hasattr(self.function_callable, "get_interface"):
            return self.function_callable.get_interface()  # type: ignore
        return function_interface_from_callable(self.function_callable)

    def source_code_language(self) -> str:
        from snapflow.core.sql.sql_function import SqlDataFunctionWrapper

        if isinstance(self.function_callable, SqlDataFunctionWrapper):
            return "sql"
        return "python"

    def get_source_code(self) -> Optional[str]:
        from snapflow.core.sql.sql_function import SqlDataFunctionWrapper

        # TODO: more principled approach (can define a "get_source_code" otherwise we inspect?)
        if isinstance(self.function_callable, SqlDataFunctionWrapper):
            return self.function_callable.sql
        if hasattr(self.function_callable, "_code"):
            return self.function_callable._code
        try:
            return inspect.getsource(self.function_callable)
        except OSError:
            # TODO: fix once we have proper file-based functions
            return ""


DataFunctionLike = Union[DataFunctionCallable, DataFunction]


def function_factory(
    function_like: Union[DataFunctionCallable, DataFunction],
    name: str = None,
    namespace: Optional[Union[SnapflowModule, str]] = None,
    **kwargs: Any,
) -> DataFunction:
    if name is None:
        assert function_like is not None
        name = make_function_name(function_like)
    if isinstance(function_like, DataFunction):
        # TODO: this is dicey, merging an existing function ... which values take precedence?
        # old_attrs = asdict(function_like)
        if isinstance(namespace, SnapflowModule):
            namespace = namespace.namespace
        else:
            namespace = namespace
        # Because we default to local module if not specified, but allow chaining decorators
        # (like DataFunction(namespace="core")(Param(...)(Param.....))) we must undo adding to default local
        # module if we later run into a specified module.
        if (
            function_like.namespace
            and namespace
            and function_like.namespace != namespace
        ):
            # We're moving modules, so make that happen here if default
            if function_like.namespace == DEFAULT_NAMESPACE:
                DEFAULT_LOCAL_MODULE.remove_function(function_like)
        namespace = namespace or function_like.namespace
        if namespace is None:
            namespace = DEFAULT_LOCAL_NAMESPACE
        function = DataFunction(
            name=name,
            namespace=namespace,
            function_callable=function_like.function_callable,
            required_storage_classes=kwargs.get("required_storage_classes")
            or function_like.required_storage_classes,
            required_storage_engines=kwargs.get("required_storage_engines")
            or function_like.required_storage_engines,
            # params=kwargs.get("params") or function_like.params,
            state_class=kwargs.get("state_class") or function_like.state_class,
            # declared_inputs=kwargs.get("declared_inputs")
            # or function_like.declared_inputs,
            # declared_output=kwargs.get("declared_output")
            # or function_like.declared_output,
            ignore_signature=kwargs.get("ignore_signature")
            or function_like.ignore_signature,
            display_name=kwargs.get("display_name") or function_like.display_name,
            description=kwargs.get("description") or function_like.description,
        )
    else:
        if namespace is None:
            namespace = DEFAULT_NAMESPACE
        if isinstance(namespace, SnapflowModule):
            namespace = namespace.namespace
        else:
            namespace = namespace
        function = DataFunction(
            name=name,
            namespace=namespace,
            function_callable=function_like,
            **kwargs,
        )
    if namespace == DEFAULT_NAMESPACE:
        # Add to default module
        DEFAULT_LOCAL_MODULE.add_function(function)
    return function


def function_decorator(
    function_or_name: Union[str, DataFunctionCallable, DataFunction] = None,
    name: str = None,
    namespace: Optional[Union[SnapflowModule, str]] = None,
    # params: List[Parameter] = None,
    state_class: Optional[Type] = None,
    **kwargs,
) -> Callable:
    if isinstance(function_or_name, str) or function_or_name is None:
        return partial(
            function_decorator,
            namespace=namespace,
            name=name or function_or_name,
            # params=params,
            state_class=state_class,
            **kwargs,
        )
    return function_factory(
        function_or_name,
        name=name,
        namespace=namespace,
        # params=params,
        state_class=state_class,
        **kwargs,
    )


def make_function(function_like: DataFunctionLike, **kwargs) -> DataFunction:
    if isinstance(function_like, DataFunction):
        return function_like
    return function_factory(function_like, **kwargs)


def ensure_function(
    env: Environment, function_like: Union[DataFunctionLike, str]
) -> DataFunction:
    if isinstance(function_like, DataFunction):
        return function_like
    if isinstance(function_like, str):
        return env.get_function(function_like)
    return make_function(function_like)


class PythonCodeDataFunctionWrapper:
    def __init__(self, code):
        self._code = code

    def get_function(self) -> DataFunction:
        local_vars = locals()
        exec(self._code, globals(), local_vars)
        function = None
        for v in local_vars.values():
            if isinstance(v, DataFunction):
                function = v
                break
        else:
            raise Exception("DataFunction not found in code")
        return function

    def get_interface(self) -> DataFunctionInterfaceCfg:
        return self.get_function().get_interface()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.get_function(), name)

    def __call__(self, *args: DataFunctionContext, **inputs: DataInterfaceType) -> Any:
        function = self.get_function()
        code = (
            self._code
            + f"\nret = {function.function_callable.__name__}(*args, **inputs)"
        )
        scope = globals()
        scope["args"] = args
        scope["inputs"] = inputs
        exec(code, scope)
        return scope["ret"]


def deprecated(*args, **kwargs):
    raise DeprecationWarning()


# Decorator API
Input = deprecated
Output = deprecated
Param = deprecated
Function = function_decorator
# function = function_decorator
_Function = DataFunction
datafunction = function_decorator
# data_function = function_decorator
