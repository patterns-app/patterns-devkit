from __future__ import annotations

import inspect
from dataclasses import asdict, dataclass, field
from functools import partial
from snapflow.core.new_pipe_interface import (
    DeclaredInput,
    DeclaredOutput,
    DeclaredPipeInterface,
    merge_declared_interface_with_signature_interface,
    pipe_interface_from_callable,
)
from snapflow.schema.base import SchemaLike
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from pandas import DataFrame
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.module import DEFAULT_LOCAL_MODULE, SnapflowModule
from snapflow.core.runtime import DatabaseRuntimeClass, PythonRuntimeClass, RuntimeClass
from snapflow.storage.data_formats import DatabaseTableRef, Records

if TYPE_CHECKING:
    from snapflow.core.execution import PipeContext
    from snapflow import Environment


class PipeException(Exception):
    pass


class InputExhaustedException(PipeException):
    pass


PipeCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame, Records, DatabaseTableRef, DataBlockMetadata, DataBlock,
]  # TODO: also input...?   Isn't this duplicated with the Interface list AND with DataFormats?


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


def make_pipe_name(pipe: Union[PipeCallable, Pipe, str]) -> str:
    # TODO: something more principled / explicit?
    if isinstance(pipe, str):
        return pipe
    if hasattr(pipe, "name"):
        return pipe.name  # type: ignore
    if hasattr(pipe, "__name__"):
        return pipe.__name__
    if hasattr(pipe, "__class__"):
        return pipe.__class__.__name__
    raise Exception(f"Cannot make name for pipe-like {pipe}")


@dataclass  # (frozen=True)
class Pipe:
    name: str
    module_name: str
    pipe_callable: Callable
    compatible_runtime_classes: List[Type[RuntimeClass]]
    config_class: Optional[Type] = None
    state_class: Optional[Type] = None
    signature_inputs: Optional[Dict[str, str]] = None
    signature_output: Optional[str] = None
    declared_inputs: Optional[List[DeclaredInput]] = None
    declared_output: Optional[DeclaredOutput] = None

    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies

    @property
    def key(self) -> str:
        k = self.name
        if self.module_name:
            k = self.module_name + "." + k
        return k

    def __call__(
        self, *args: PipeContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        return self.pipe_callable(*args, **kwargs)

    def get_interface(self) -> DeclaredPipeInterface:
        """"""
        found_signature_interface = self._get_pipe_interface()
        declared_interface = DeclaredPipeInterface(
            inputs=self.declared_inputs or [], output=self.declared_output
        )
        return merge_declared_interface_with_signature_interface(
            declared_interface, found_signature_interface
        )

    def _get_pipe_interface(self) -> DeclaredPipeInterface:
        if hasattr(self.pipe_callable, "get_interface"):
            return self.pipe_callable.get_interface()  # type: ignore
        return pipe_interface_from_callable(self.pipe_callable)

    def source_code_language(self) -> str:
        from snapflow.core.sql.pipe import SqlPipeWrapper

        if isinstance(self.pipe_callable, SqlPipeWrapper):
            return "sql"
        return "python"

    def get_source_code(self) -> Optional[str]:
        from snapflow.core.sql.pipe import SqlPipeWrapper

        # TODO: more principled approach (can define a "get_source_code" otherwise we inspect?)
        if isinstance(self.pipe_callable, SqlPipeWrapper):
            return self.pipe_callable.sql
        return inspect.getsource(self.pipe_callable)


PipeLike = Union[PipeCallable, Pipe]


def pipe_factory(
    pipe_like: Union[PipeCallable, Pipe],
    name: str = None,
    module: Optional[Union[SnapflowModule, str]] = None,
    compatible_runtimes: str = None,
    **kwargs: Any,
) -> Pipe:
    if name is None:
        assert pipe_like is not None
        name = make_pipe_name(pipe_like)
    runtime_class = get_runtime_class(compatible_runtimes)
    if module is None:
        module = DEFAULT_LOCAL_MODULE
    if isinstance(module, SnapflowModule):
        module_name = module.name
    else:
        module_name = module
    if isinstance(pipe_like, Pipe):
        old_attrs = asdict(pipe_like)
        if module_name is not None:
            old_attrs["module_name"] = module_name
        if compatible_runtimes is not None:
            old_attrs["compatible_runtime_classes"] = runtime_class
        old_attrs.update(**kwargs)
        return Pipe(**old_attrs)

    return Pipe(
        name=name,
        module_name=module_name,
        pipe_callable=pipe_like,
        compatible_runtime_classes=[runtime_class],
        **kwargs,
    )


def pipe(
    pipe_or_name: Union[str, PipeCallable, Pipe] = None,
    name: str = None,
    module: Optional[Union[SnapflowModule, str]] = None,
    compatible_runtimes: str = None,
    config_class: Optional[Type] = None,
    state_class: Optional[Type] = None,
    # inputs: Optional[Dict[str, str]] = None,
    # output: Optional[str] = None,
) -> Union[Callable, Pipe]:
    if isinstance(pipe_or_name, str) or pipe_or_name is None:
        return partial(
            pipe,
            module=module,
            name=pipe_or_name,
            compatible_runtimes=compatible_runtimes,
            config_class=config_class,
            state_class=state_class,
            # inputs=inputs,
            # output=output,
        )
    return pipe_factory(
        pipe_or_name,
        name=name,
        module=module,
        compatible_runtimes=compatible_runtimes,
        config_class=config_class,
        state_class=state_class,
        # inputs=inputs,
        # output=output,
    )


def add_declared_input(
    name: str,
    schema: Optional[SchemaLike] = None,
    reference: bool = False,
    required: bool = True,
    from_self: bool = False,  # TODO: name
    stream: bool = False,
):
    inpt = DeclaredInput(
        name=name,
        schema_like=schema or Any,
        reference=reference,
        required=required,
        from_self=from_self,
        stream=stream,
    )

    def dec(pipe_like: Union[PipeCallable, Pipe]) -> Pipe:
        if not isinstance(pipe_like, Pipe):
            pipe_like = pipe_factory(pipe_like)
        pipe: Pipe = pipe_like
        pipe.declared_inputs.append(inpt)
        return pipe

    return dec


input = add_declared_input


def add_declared_output(
    schema: Optional[SchemaLike] = None,
    optional: bool = False,
    name: Optional[str] = None,
    stream: bool = False,
    default: bool = True,
):
    output = DeclaredOutput(
        name=name,
        schema_like=schema or Any,
        optional=optional,
        stream=stream,
        default=default,
    )

    def dec(pipe_like: Union[PipeCallable, Pipe]) -> Pipe:
        if not isinstance(pipe_like, Pipe):
            pipe_like = pipe_factory(pipe_like)
        pipe: Pipe = pipe_like
        pipe.declared_output = output
        return pipe

    return dec


output = add_declared_output


def make_pipe(pipe_like: PipeLike, **kwargs) -> Pipe:
    if isinstance(pipe_like, Pipe):
        return pipe_like
    return pipe_factory(pipe_like, **kwargs)


def ensure_pipe(env: Environment, pipe_like: Union[PipeLike, str]) -> Pipe:
    if isinstance(pipe_like, Pipe):
        return pipe_like
    if isinstance(pipe_like, str):
        return env.get_pipe(pipe_like)
    return make_pipe(pipe_like)

