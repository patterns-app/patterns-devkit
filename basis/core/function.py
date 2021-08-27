from __future__ import annotations

import inspect
import typing
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from basis.cli.commands import output
from basis.core.block import Block
from basis.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    BlockType,
    FunctionCfg,
    FunctionInterfaceCfg,
    Generic,
    IoBaseCfg,
    Parameter,
    ParameterCfg,
    Table,
    is_record_like,
)
from basis.utils.docstring import BasisParser, Docstring
from dcp.data_format.formats.memory.records import Records
from pandas import DataFrame

if TYPE_CHECKING:
    from basis import Context
    from basis import Environment


class FunctionException(Exception):
    pass


class InputExhaustedException(FunctionException):
    pass


FunctionCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame,
    Records,
    Block,
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
    parameters: typing.OrderedDict[str, ParameterCfg] = field(
        default_factory=OrderedDict
    )
    # stdout: Optional[str] = None
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
            inputs=self.inputs,
            outputs=self.outputs,
            parameters=self.parameters,
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
    def params(self) -> Dict[str, ParameterCfg]:
        return self.get_interface().parameters

    def get_param(self, name: str) -> ParameterCfg:
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


def parse_docstring(d: str) -> Docstring:
    return BasisParser().parse(d)


def function_interface_from_callable(
    function: FunctionCallable,
) -> FunctionInterfaceCfg:
    if hasattr(function, "get_interface"):
        return function.get_interface()
    signature = inspect.signature(function)
    outputs = OrderedDict()
    outputs[DEFAULT_OUTPUT_NAME] = IoBaseCfg(
        name=DEFAULT_OUTPUT_NAME, block_type=BlockType.Generic
    )
    inputs = OrderedDict()
    first = True
    for name, param in signature.parameters.items():
        optional = param.default is None
        if first:
            # First input could be stream or table
            inputs[name] = IoBaseCfg(
                name=name, block_type=BlockType.Generic, required=not optional
            )
            first = False
        else:
            # Additional inputs must be table ref
            inputs[name] = IoBaseCfg(
                name=name, block_type=BlockType.Table, required=not optional
            )
    return FunctionInterfaceCfg(
        inputs=inputs,
        outputs=outputs,
    )


def wrap_simple_function(function_callable: Callable) -> Callable:
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


def make_function_from_simple_function(
    function_callable: Callable,
    name: Optional[str] = None,
    inputs: typing.OrderedDict[str, IoBaseCfg] = None,
    outputs: typing.OrderedDict[str, IoBaseCfg] = None,
    parameters: typing.OrderedDict[str, ParameterCfg] = None,
    **kwargs,
) -> Function:
    if name is None:
        name = make_function_name(function_callable)
    assert callable(function_callable)
    interface = function_interface_from_callable(function_callable)
    wrapped_fn = wrap_simple_function(function_callable)
    doc = parse_docstring(function_callable.__doc__)
    return Function(
        name=name,
        function_callable=wrapped_fn,
        inputs=inputs or interface.inputs,
        outputs=outputs or interface.outputs,
        parameters=parameters or interface.parameters,
        description=doc.short_description,
        **kwargs,
    )


def ensure_function(function_like, **kwargs) -> Function:
    if isinstance(function_like, Function):
        return function_like
    elif callable(function_like):
        return make_function_from_simple_function(function_like)
    raise TypeError(f"Function must be callable")


def list_to_ordered_dict(lst: List) -> OrderedDict:
    return OrderedDict([(i.name, i) for i in lst or []])


def function_decorator(
    function_or_name: Union[str, FunctionCallable, Function] = None,
    name: str = None,
    inputs: List[IoBaseCfg] = None,
    outputs: List[IoBaseCfg] = None,
    parameters: List[ParameterCfg] = None,
    **kwargs,
) -> Union[Function, FunctionCallable]:
    if isinstance(function_or_name, str) or function_or_name is None:
        return partial(
            function_decorator,
            name=name or function_or_name,
            inputs=inputs,
            outputs=outputs,
            parameters=parameters,
            **kwargs,
        )
    fn = function_or_name
    if name is None:
        name = make_function_name(fn)
    inputs_od = list_to_ordered_dict(inputs or [])
    outputs_od = list_to_ordered_dict(outputs or [])
    parameters_od = list_to_ordered_dict(parameters or [])
    return Function(
        name=name,
        function_callable=fn,
        inputs=inputs_od,
        outputs=outputs_od,
        parameters=parameters_od,
        **kwargs,
    )


def simple_function_decorator(
    function_or_name: Union[str, FunctionCallable, Function] = None,
    name: str = None,
    inputs: List[IoBaseCfg] = None,
    outputs: List[IoBaseCfg] = None,
    parameters: List[ParameterCfg] = None,
    **kwargs,
) -> Union[Function, Callable]:
    if isinstance(function_or_name, str) or function_or_name is None:
        return partial(
            function_decorator,
            name=name or function_or_name,
            inputs=inputs,
            outputs=outputs,
            parameters=parameters,
            **kwargs,
        )
    inputs_od = list_to_ordered_dict(inputs or [])
    outputs_od = list_to_ordered_dict(outputs or [])
    parameters_od = list_to_ordered_dict(parameters or [])
    return make_function_from_simple_function(
        function_or_name,
        name=name,
        inputs=inputs_od,
        outputs=outputs_od,
        parameters=parameters_od,
        **kwargs,
    )


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


# Decorator API
function = function_decorator
simple_function = simple_function_decorator
