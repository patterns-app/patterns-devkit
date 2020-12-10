from __future__ import annotations

import inspect
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from pandas import DataFrame
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.data_formats import DatabaseTableRef, RecordsList
from snapflow.core.module import DEFAULT_LOCAL_MODULE, SnapflowModule
from snapflow.core.pipe_interface import PipeAnnotation, PipeInterface
from snapflow.core.runtime import RuntimeClass

if TYPE_CHECKING:
    from snapflow.core.runnable import PipeContext
    from snapflow import Environment


class PipeException(Exception):
    pass


class InputExhaustedException(PipeException):
    pass


PipeCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame,
    RecordsList,
    DatabaseTableRef,
    DataBlockMetadata,
    DataBlock,
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


def make_pipe_name(pipe: Union[PipeCallable, str]) -> str:
    # TODO: something more principled / explicit?
    if isinstance(pipe, str):
        return pipe
    if hasattr(pipe, "name"):
        return pipe.name  # type: ignore
    if hasattr(pipe, "__name__"):
        return pipe.__name__
    if hasattr(pipe, "__class__"):
        return pipe.__class__.__name__
    raise Exception(f"Invalid Pipe name {pipe}")


@dataclass(frozen=True)
class Pipe:
    name: str
    module_name: str
    pipe_callable: Callable
    compatible_runtime_classes: List[RuntimeClass]
    config_class: Optional[Type] = None
    state_class: Optional[Type] = None
    declared_inputs: Optional[Dict[str, str]] = None
    declared_output: Optional[str] = None

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

    def get_interface(self) -> PipeInterface:
        """"""
        found_interface = self._get_pipe_interface()
        assert found_interface is not None
        declared_interface = self._get_declared_interface()
        # Merge found and declared
        inputs = found_interface.inputs
        output = found_interface.output
        if declared_interface.inputs:
            inputs = declared_interface.inputs
        if declared_interface.output is not None:
            output = declared_interface.output
        return PipeInterface(
            inputs=inputs,
            output=output,
            requires_pipe_context=found_interface.requires_pipe_context,
        )

    def _get_pipe_interface(self) -> PipeInterface:
        if hasattr(self.pipe_callable, "get_interface"):
            return self.pipe_callable.get_interface()  # type: ignore
        return PipeInterface.from_pipe_definition(self.pipe_callable)

    def _get_declared_interface(self) -> PipeInterface:
        inputs = []
        if self.declared_inputs:
            for name, annotation in self.declared_inputs.items():
                inputs.append(
                    PipeAnnotation.from_type_annotation(annotation, name=name)
                )
        output = None
        if self.declared_output:
            output = PipeAnnotation.from_type_annotation(self.declared_output)
        return PipeInterface(
            inputs=inputs,
            output=output,
        )

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
    pipe_callable: PipeCallable,
    name: str = None,
    module: Optional[Union[SnapflowModule, str]] = None,
    compatible_runtimes: str = None,
    inputs: Optional[Dict[str, str]] = None,
    output: Optional[str] = None,
    **kwargs: Any,
) -> Pipe:
    if name is None:
        if pipe_callable is None:
            raise
        name = make_pipe_name(pipe_callable)
    runtime_class = get_runtime_class(compatible_runtimes)
    if module is None:
        module = DEFAULT_LOCAL_MODULE
    if isinstance(module, SnapflowModule):
        module_name = module.name
    else:
        module_name = module
    return Pipe(
        name=name,
        module_name=module_name,
        pipe_callable=pipe_callable,
        compatible_runtime_classes=[runtime_class],
        declared_inputs=inputs,
        declared_output=output,
        **kwargs,
    )


def pipe(
    pipe_or_name: Union[str, PipeCallable] = None,
    name: str = None,
    module: Optional[Union[SnapflowModule, str]] = None,
    compatible_runtimes: str = None,
    config_class: Optional[Type] = None,
    state_class: Optional[Type] = None,
    inputs: Optional[Dict[str, str]] = None,
    output: Optional[str] = None,
) -> Union[Callable, Pipe]:
    if isinstance(pipe_or_name, str) or pipe_or_name is None:
        return partial(
            pipe,
            module=module,
            name=pipe_or_name,
            compatible_runtimes=compatible_runtimes,
            config_class=config_class,
            state_class=state_class,
            inputs=inputs,
            output=output,
        )
    return pipe_factory(
        pipe_or_name,
        name=name,
        module=module,
        compatible_runtimes=compatible_runtimes,
        config_class=config_class,
        state_class=state_class,
        inputs=inputs,
        output=output,
    )


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
