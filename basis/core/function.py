from __future__ import annotations
from collections import OrderedDict
import typing
from basis.cli.commands import output

import inspect
from dataclasses import asdict, dataclass, field
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from basis.core.component import (
    DEFAULT_LOCAL_NAMESPACE,
    DEFAULT_NAMESPACE,
    global_library,
)
from basis.core.block import Block
from basis.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    BlockType,
    FunctionCfg,
    FunctionInterfaceCfg,
    Generic,
    IoBaseCfg,
    Table,
    is_record_like,
)
from basis.core.function_interface import (  # merge_declared_interface_with_signature_interface,
    Parameter,
    function_interface_from_callable,
)
from basis.core.module import DEFAULT_LOCAL_MODULE, BasisModule
from basis.core.runtime import DatabaseRuntimeClass, PythonRuntimeClass, RuntimeClass
from commonmodel.base import SchemaLike
from dcp.data_format.formats.memory.records import Records
from pandas import DataFrame

if TYPE_CHECKING:
    from basis import Context
    from basis import Environment
    from basis.core.function_package import FunctionPackage


class FunctionException(Exception):
    pass


class InputExhaustedException(FunctionException):
    pass


FunctionCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame, Records, Block,
]  # TODO: also input...?   Isn't this duplicated with the Interface list AND with DataFormats?


def make_function_name(function: Union[FunctionCallable, Function, str]) -> str:
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
class Function:
    # Underscored so the decorator API can use `Function`. TODO: Is there a better way / name?
    name: str
    function_callable: Callable
    required_storage_classes: List[str] = field(default_factory=list)
    required_storage_engines: List[str] = field(default_factory=list)
    inputs: typing.OrderedDict[str, IoBaseCfg] = field(default_factory=OrderedDict)
    outputs: typing.OrderedDict[str, IoBaseCfg] = field(default_factory=OrderedDict)
    parameters: List[Parameter] = field(default_factory=list)
    # compatible_runtime_classes: List[Type[RuntimeClass]]
    display_name: Optional[str] = None
    description: Optional[str] = None
    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies

    # def __post_init__(self, *args, **kwargs):
    #     global_library.add_function(self)

    # @property
    # def key(self) -> str:
    #     k = self.name
    #     if self.namespace:
    #         k = self.namespace + "." + k
    #     return k

    def __call__(
        self, *args: Context, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        return self.function_callable(*args, **kwargs)

    # def get_original_object(self) -> Any:
    #     return self._original_object or self.function_callable

    def get_interface(self) -> FunctionInterfaceCfg:
        return FunctionInterfaceCfg(
            inputs=self.inputs, outputs=self.outputs, parameters=self.parameters,
        )
        # found_signature_interface = self._get_function_interface()
        # return found_signature_interface
        # declared_interface = FunctionInterface(
        #     inputs=self.declared_inputs or [], output=self.declared_output
        # )
        # return merge_declared_interface_with_signature_interface(
        #     declared_interface,
        #     found_signature_interface,
        #     ignore_signature=self.ignore_signature,
        # )

    def to_config(self) -> FunctionCfg:
        return FunctionCfg(
            name=self.name,
            interface=self.get_interface(),
            required_storage_classes=self.required_storage_classes,
            required_storage_engines=self.required_storage_engines,
        )

    @property
    def params(self) -> Dict[str, Parameter]:
        return self.get_interface().parameters

    def get_param(self, name: str) -> Parameter:
        return self.get_interface().parameters[name]

    # def _get_function_interface(self) -> FunctionInterfaceCfg:
    #     if hasattr(self.function_callable, "get_interface"):
    #         return self.function_callable.get_interface()  # type: ignore
    #     return function_interface_from_callable(self.function_callable)

    def source_code_language(self) -> str:
        from basis.core.sql.sql_function import SqlFunctionWrapper

        if isinstance(self.function_callable, SqlFunctionWrapper):
            return "sql"
        return "python"

    def get_source_code(self) -> Optional[str]:
        from basis.core.sql.sql_function import SqlFunctionWrapper

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


FunctionLike = Union[FunctionCallable, Function]


def function_interface_from_callable(
    function: FunctionCallable,
) -> FunctionInterfaceCfg:
    signature = inspect.signature(function)
    outputs = OrderedDict()
    outputs[DEFAULT_OUTPUT_NAME] = IoBaseCfg(
        name=DEFAULT_OUTPUT_NAME, block_type=BlockType.Generic
    )
    inputs = OrderedDict()
    first = True
    for name in signature.parameters:
        if first:
            # First input could be stream or table
            inputs[name] = IoBaseCfg(name=name, block_type=BlockType.Generic)
            first = False
        else:
            # Additional inputs must be table ref
            inputs[name] = IoBaseCfg(name=name, block_type=BlockType.Table)
    return FunctionInterfaceCfg(inputs=inputs, outputs=outputs,)


def wrap_bare_function(function_callable: Callable) -> Callable:
    @wraps(function_callable)
    def function_wrapper(ctx: Context):
        if not ctx.inputs:
            # Generative function, just emit output
            result = function_callable()
            ctx.emit(result)
            return
        input_names = list(ctx.inputs)
        inputs = list(ctx.inputs.values())
        first_input = inputs[0]
        other_inputs = inputs[1:]
        for i in other_inputs:
            # Other inputs must be refs
            assert not isinstance(i, list)
        other_args = [i for i in other_inputs]
        for block in first_input:
            args = [block]
            args.extend(other_args)
            try:
                result = function_callable(*args)
                ctx.emit(result)
                ctx.consume(input_names[0], block)
            except Exception as e:
                ctx.emit_error(block, str(e))  # TODO: function traceback

    return function_wrapper


def function_factory(
    function_like: Union[FunctionCallable, Function], name: str = None, **kwargs: Any,
) -> Function:
    if name is None:
        assert function_like is not None
        name = make_function_name(function_like)
    assert callable(function_like)
    interface = function_interface_from_callable(function_like)
    wrapped_fn = wrap_bare_function(function_like)
    return Function(
        name=name,
        function_callable=wrapped_fn,
        inputs=interface.inputs,
        outputs=interface.outputs,
    )
    # if isinstance(function_like, Function):
    #     function = Function(
    #         name=name,
    #         function_callable=function_like.function_callable,
    #         required_storage_classes=kwargs.get("required_storage_classes")
    #         or function_like.required_storage_classes,
    #         required_storage_engines=kwargs.get("required_storage_engines")
    #         or function_like.required_storage_engines,
    #         # params=kwargs.get("params") or function_like.params,
    #         # declared_inputs=kwargs.get("declared_inputs")
    #         # or function_like.declared_inputs,
    #         # declared_output=kwargs.get("declared_output")
    #         # or function_like.declared_output,
    #         or function_like.ignore_signature,
    #         display_name=kwargs.get("display_name") or function_like.display_name,
    #         description=kwargs.get("description") or function_like.description,
    #     )
    # else:
    #     if namespace is None:
    #         namespace = DEFAULT_NAMESPACE
    #     if isinstance(namespace, BasisModule):
    #         namespace = namespace.namespace
    #     else:
    #         namespace = namespace
    #     function = Function(
    #         name=name, namespace=namespace, function_callable=function_like, **kwargs,
    #     )
    # if namespace == DEFAULT_NAMESPACE:
    #     # Add to default module
    #     DEFAULT_LOCAL_MODULE.add_function(function)
    # return function


def function_decorator(
    function_or_name: Union[str, FunctionCallable, Function] = None,
    name: str = None,
    inputs: List[IoBaseCfg] = None,
    outputs: List[IoBaseCfg] = None,
    parameters: List[Parameter] = None,
    **kwargs,
) -> Callable:
    if isinstance(function_or_name, str) or function_or_name is None:
        return partial(
            function_decorator,
            name=name or function_or_name,
            inputs=inputs,
            outputs=outputs,
            parameters=parameters,
            **kwargs,
        )
    return function_factory(
        function_or_name,
        name=name,
        inputs=inputs,
        outputs=outputs,
        parameters=parameters,
        **kwargs,
    )


def make_function(function_like: FunctionLike, **kwargs) -> Function:
    if isinstance(function_like, Function):
        return function_like
    return function_factory(function_like, **kwargs)


def ensure_function(
    env: Environment, function_like: Union[FunctionLike, str]
) -> Function:
    if isinstance(function_like, Function):
        return function_like
    if isinstance(function_like, str):
        return env.get_function(function_like)
    return make_function(function_like)


class PythonCodeFunctionWrapper:
    def __init__(self, code):
        self._code = code

    def get_function(self) -> Function:
        local_vars = locals()
        exec(self._code, globals(), local_vars)
        function = None
        for v in local_vars.values():
            if isinstance(v, Function):
                function = v
                break
        else:
            raise Exception("Function not found in code")
        return function

    def get_interface(self) -> FunctionInterfaceCfg:
        return self.get_function().get_interface()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.get_function(), name)

    def __call__(self, *args: Context, **inputs: DataInterfaceType) -> Any:
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
function = function_decorator
# Function = function_decorator
# function = function_decorator
_Function = Function
datafunction = function_decorator
# data_function = function_decorator
