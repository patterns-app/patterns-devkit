from __future__ import annotations

import inspect
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from commonmodel.base import SchemaLike
from dcp.data_format.formats.memory.records import Records
from pandas import DataFrame
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.function_interface import (  # merge_declared_interface_with_signature_interface,
    FunctionInput,
    FunctionInterface,
    FunctionOutput,
    Parameter,
    function_interface_from_callable,
)
from snapflow.core.module import (
    DEFAULT_LOCAL_MODULE,
    DEFAULT_LOCAL_NAMESPACE,
    DEFAULT_NAMESPACE,
    SnapflowModule,
)
from snapflow.core.runtime import DatabaseRuntimeClass, PythonRuntimeClass, RuntimeClass

if TYPE_CHECKING:
    from snapflow.core.execution import FunctionContext
    from snapflow import Environment
    from snapflow.core.function_package import FunctionPackage


class FunctionException(Exception):
    pass


class InputExhaustedException(FunctionException):
    pass


FunctionCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame,
    Records,
    DataBlockMetadata,
    DataBlock,
]  # TODO: also input...?   Isn't this duplicated with the Interface list AND with DataFormats?

DEFAULT_OUTPUT_NAME = "default"


def get_runtime_class(runtime: Optional[str]) -> Type[RuntimeClass]:
    if runtime is None:
        return PythonRuntimeClass
    if (
        "ql" in runtime.lower()
        or "database" in runtime.lower()
        or "postgre" in runtime.lower()
    ):
        return DatabaseRuntimeClass
    return PythonRuntimeClass


def make_function_name(function: Union[FunctionCallable, _Function, str]) -> str:
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


@dataclass  # (frozen=True)
class _Function:
    # Underscored so the decorator API can use `Function`. TODO: Is there a better way / name?
    name: str
    namespace: str
    function_callable: Callable
    required_storage_classes: List[str] = field(default_factory=list)
    required_storage_engines: List[str] = field(default_factory=list)
    # compatible_runtime_classes: List[Type[RuntimeClass]]
    # params: List[Parameter] = field(default_factory=list)
    state_class: Optional[Type] = None
    # declared_inputs: Optional[List[FunctionInput]] = None
    # declared_output: Optional[FunctionOutput] = None
    ignore_signature: bool = (
        False  # Whether to ignore signature if there are any declared i/o
    )
    _original_object: Any = None
    package: FunctionPackage = None
    display_name: Optional[str] = None
    description: Optional[str] = None

    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies

    @property
    def key(self) -> str:
        k = self.name
        if self.namespace:
            k = self.namespace + "." + k
        return k

    def __call__(
        self, *args: FunctionContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        return self.function_callable(*args, **kwargs)

    def get_original_object(self) -> Any:
        return self._original_object or self.function_callable

    def get_interface(self) -> FunctionInterface:
        """"""
        found_signature_interface = self._get_function_interface()
        return found_signature_interface
        # declared_interface = FunctionInterface(
        #     inputs=self.declared_inputs or [], output=self.declared_output
        # )
        # return merge_declared_interface_with_signature_interface(
        #     declared_interface,
        #     found_signature_interface,
        #     ignore_signature=self.ignore_signature,
        # )

    @property
    def params(self) -> Dict[str, Parameter]:
        return self.get_interface().parameters

    def get_param(self, name: str) -> Parameter:
        return self.get_interface().parameters[name]

    def _get_function_interface(self) -> FunctionInterface:
        if hasattr(self.function_callable, "get_interface"):
            return self.function_callable.get_interface()  # type: ignore
        return function_interface_from_callable(self.function_callable)

    def source_code_language(self) -> str:
        from snapflow.core.sql.sql_function import SqlFunctionWrapper

        if isinstance(self.function_callable, SqlFunctionWrapper):
            return "sql"
        return "python"

    def get_source_code(self) -> Optional[str]:
        from snapflow.core.sql.sql_function import SqlFunctionWrapper

        # TODO: more principled approach (can define a "get_source_code" otherwise we inspect?)
        if isinstance(self.function_callable, SqlFunctionWrapper):
            return self.function_callable.sql
        if hasattr(self.function_callable, "_code"):
            return self.function_callable._code
        try:
            return inspect.getsource(self.function_callable)
        except OSError:
            # TODO: fix once we have proper file-based functions
            return ""


FunctionLike = Union[FunctionCallable, _Function]


def function_factory(
    function_like: Union[FunctionCallable, _Function],
    name: str = None,
    namespace: Optional[Union[SnapflowModule, str]] = None,
    **kwargs: Any,
) -> _Function:
    if name is None:
        assert function_like is not None
        name = make_function_name(function_like)
    if isinstance(function_like, _Function):
        # TODO: this is dicey, merging an existing function ... which values take precedence?
        # old_attrs = asdict(function_like)
        if isinstance(namespace, SnapflowModule):
            namespace = namespace.namespace
        else:
            namespace = namespace
        # Because we default to local module if not specified, but allow chaining decorators
        # (like Function(namespace="core")(Param(...)(Param.....))) we must undo adding to default local
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
        function = _Function(
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
        function = _Function(
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
    function_or_name: Union[str, FunctionCallable, _Function] = None,
    name: str = None,
    namespace: Optional[Union[SnapflowModule, str]] = None,
    # params: List[Parameter] = None,
    state_class: Optional[Type] = None,
    **kwargs,
) -> Union[Callable, _Function]:
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


# def add_declared_input_decorator(
#     name: str,
#     schema: Optional[SchemaLike] = None,
#     reference: bool = False,
#     required: bool = True,
#     from_self: bool = False,  # TODO: name
#     stream: bool = False,
# ):
#     inpt = FunctionInput(
#         name=name,
#         schema_like=schema or "Any",
#         reference=reference,
#         _required=required,
#         from_self=from_self,
#         stream=stream,
#     )

#     def dec(function_like: Union[FunctionCallable, _Function]) -> _Function:
#         if not isinstance(function_like, _Function):
#             function_like = function_factory(function_like)
#         function: _Function = function_like
#         if function.declared_inputs is None:
#             function.declared_inputs = [inpt]
#         else:
#             function.declared_inputs.append(inpt)
#         return function

#     return dec


# def add_declared_output_decorator(
#     schema: Optional[SchemaLike] = None,
#     optional: bool = False,
#     name: Optional[str] = None,
#     stream: bool = False,
#     default: bool = True,
# ):
#     output = FunctionOutput(
#         name=name,
#         schema_like=schema or "Any",
#         optional=optional,
#         stream=stream,
#         default=default,
#     )

#     def dec(function_like: Union[FunctionCallable, _Function]) -> _Function:
#         if not isinstance(function_like, _Function):
#             function_like = function_factory(function_like)
#         function: _Function = function_like
#         function.declared_output = output
#         return function

#     return dec


# def add_param_decorator(
#     name: str,
#     datatype: str,
#     required: bool = False,
#     default: Any = None,
#     help: str = "",
# ):
#     p = Parameter(
#         name=name, datatype=datatype, required=required, default=default, help=help,
#     )

#     def dec(function_like: Union[FunctionCallable, _Function]) -> _Function:
#         if not isinstance(function_like, _Function):
#             function_like = function_factory(function_like)
#         function: _Function = function_like
#         if function.params is None:
#             function.params = [p]
#         else:
#             function.params.append(p)
#         return function

#     return dec


def make_function(function_like: FunctionLike, **kwargs) -> _Function:
    if isinstance(function_like, _Function):
        return function_like
    return function_factory(function_like, **kwargs)


def ensure_function(
    env: Environment, function_like: Union[FunctionLike, str]
) -> _Function:
    if isinstance(function_like, _Function):
        return function_like
    if isinstance(function_like, str):
        return env.get_function(function_like)
    return make_function(function_like)


class PythonCodeFunctionWrapper:
    def __init__(self, code):
        self._code = code

    def get_function(self) -> _Function:
        local_vars = locals()
        exec(self._code, globals(), local_vars)
        function = None
        for v in local_vars.values():
            if isinstance(v, _Function):
                function = v
                break
        else:
            raise Exception("Function not found in code")
        return function

    def get_interface(self) -> FunctionInterface:
        return self.get_function().get_interface()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.get_function(), name)

    def __call__(self, *args: FunctionContext, **inputs: DataInterfaceType) -> Any:
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
