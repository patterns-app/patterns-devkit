from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union, cast

from pandas import DataFrame

from basis.core.component import ComponentType, ComponentUri
from basis.core.data_block import DataBlockMetadata, DataSetMetadata
from basis.core.data_formats import DatabaseTableRef, RecordsList
from basis.core.module import DEFAULT_LOCAL_MODULE, BasisModule
from basis.core.pipe_interface import PipeAnnotation, PipeInterface
from basis.core.runtime import RuntimeClass

if TYPE_CHECKING:
    from basis.core.runnable import PipeContext
    from basis import Environment


class PipeException(Exception):
    pass


class InputExhaustedException(PipeException):
    pass


PipeCallable = Callable[..., Any]

DataInterfaceType = Union[
    DataFrame, RecordsList, DatabaseTableRef, DataBlockMetadata, DataSetMetadata
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


def make_pipe_name(pipe: PipeCallable) -> str:
    # TODO: something more principled / explicit?
    if hasattr(pipe, "name"):
        return pipe.name  # type: ignore
    if hasattr(pipe, "__name__"):
        return pipe.__name__
    if hasattr(pipe, "__class__"):
        return pipe.__class__.__name__
    raise Exception(f"Invalid Pipe name {pipe}")


@dataclass(frozen=True)
class Pipe(ComponentUri):
    # component_type = ComponentType.Pipe
    runtime_pipes: Dict[RuntimeClass, PipeDefinition] = field(default_factory=dict)

    @property
    def is_composite(self) -> bool:
        return self.get_representative_definition().is_composite

    @property
    def compatible_runtime_classes(self) -> List[RuntimeClass]:
        return list(self.runtime_pipes.keys())

    def __call__(
        self, *args: PipeContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        raise NotImplementedError
        # # TODO: hack, but maybe useful to have actual Pipe still act like a pipe
        # for v in self.runtime_pipes.values():
        #     try:
        #         return v(*args, **kwargs)
        #     except:
        #         pass

    def get_representative_definition(self) -> PipeDefinition:
        return list(self.runtime_pipes.values())[0]

    def get_interface(self, env: Environment) -> Optional[PipeInterface]:
        dfd = self.get_representative_definition()
        if not dfd:
            raise
        return dfd.get_interface(env)

    def add_definition(self, df: PipeDefinition):
        for cls in df.compatible_runtime_classes:
            self.runtime_pipes[cls] = df

    def validate(self):
        # TODO: check if all pipe signatures match
        pass

    def merge(self, other: ComponentUri, overwrite: bool = False):
        other = cast(Pipe, other)
        for cls, df in other.runtime_pipes.items():
            if cls not in self.runtime_pipes:
                self.runtime_pipes[cls] = df

    def get_definition(self, runtime_cls: RuntimeClass) -> Optional[PipeDefinition]:
        return self.runtime_pipes.get(runtime_cls)

    def associate_with_module(self, module: BasisModule) -> ComponentUri:
        dfds: Dict[RuntimeClass, PipeDefinition] = {}
        for cls, dfd in self.runtime_pipes.items():
            dfds[cls] = dfd.associate_with_module(module)
        return self.clone(module_name=module.name, runtime_pipes=dfds)


@dataclass(frozen=True)
class PipeDefinition(ComponentUri):
    # component_type = ComponentType.Pipe
    pipe_callable: Optional[
        Callable
    ]  # Optional since Composite DFs don't have a Callable
    compatible_runtime_classes: List[RuntimeClass]
    is_composite: bool = False
    config_class: Optional[Type] = None
    state_class: Optional[Type] = None
    declared_inputs: Optional[Dict[str, str]] = None
    declared_output: Optional[str] = None
    sub_graph: List[ComponentUri] = field(
        default_factory=list
    )  # TODO: support proper graphs

    # TODO: runtime engine eg "mysql>=8.0", "python==3.7.4"  ???
    # TODO: runtime dependencies

    def __call__(
        self, *args: PipeContext, **kwargs: DataInterfaceType
    ) -> Optional[DataInterfaceType]:
        if self.is_composite:
            raise NotImplementedError(f"Cannot call a composite Pipe {self}")
        if self.pipe_callable is None:
            raise
        return self.pipe_callable(*args, **kwargs)

    def get_interface(self, env: Environment) -> Optional[PipeInterface]:
        """
        """
        found_dfi = self._get_pipe_interface(env)
        declared_dfi = self._get_declared_interface()
        declared_dfi.requires_pipe_context = (
            found_dfi.requires_pipe_context
        )  # TODO: or require explicit?
        # Override any found annotations with declared ones
        # for input in declared_dfi.inputs:
        #     try:
        #         found_input = found_dfi.get_input(input.name)
        #         found_input.data_format_class = input.data_format_class
        #         found_input.otype_like = input.otype_like
        #     except KeyError:
        #         found_dfi.inputs.append(input)
        # if declared_dfi.output:
        #     if found_dfi.output:
        #         found_dfi.output.data_format_class = (
        #             declared_dfi.output.data_format_class
        #         )
        #         found_dfi.output.otype_like = declared_dfi.output.otype_like
        #     else:
        #         found_dfi.output = declared_dfi.output
        if self.declared_output is not None or self.declared_inputs is not None:
            return declared_dfi
        return found_dfi

    def _get_pipe_interface(self, env: Environment) -> Optional[PipeInterface]:
        if self.pipe_callable is None:
            assert self.is_composite
            # TODO: only supports chain
            input = ensure_pipe(env, self.sub_graph[0])
            output = ensure_pipe(env, self.sub_graph[-1])
            input_interface = input.get_interface(env)
            return PipeInterface(
                inputs=input_interface.inputs,
                output=output.get_interface(env).output,
                requires_pipe_context=input_interface.requires_pipe_context,
            )
        if hasattr(self.pipe_callable, "get_interface"):
            return self.pipe_callable.get_interface()
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
        return PipeInterface(inputs=inputs, output=output,)

    def associate_with_module(self, module: BasisModule) -> ComponentUri:
        new_subs = []
        if self.sub_graph:
            for sub in self.sub_graph:
                new_subs.append(sub.associate_with_module(module))
        return self.clone(module_name=module.name, sub_graph=new_subs)

    def as_pipe(self) -> Pipe:
        df = Pipe(
            component_type=ComponentType.Pipe,
            name=self.name,
            module_name=self.module_name,
            version=self.version,
        )
        df.add_definition(self)
        return df

    def get_source_code(self) -> Optional[str]:
        from basis.core.sql.pipe import SqlPipeWrapper

        # TODO: more principled approach (can define a "get_source_code" otherwise we inspect?)
        if self.pipe_callable is not None:
            if isinstance(self.pipe_callable, SqlPipeWrapper):
                return self.pipe_callable.sql
            return inspect.getsource(self.pipe_callable)
        return None


PipeDefinitionLike = Union[PipeCallable, PipeDefinition]
PipeLike = Union[PipeCallable, PipeDefinition, Pipe]


def pipe_definition_factory(
    pipe_callable: Optional[PipeCallable],  # Composite DFs don't have a callable
    name: str = None,
    version: str = None,
    compatible_runtimes: str = None,
    module_name: str = None,
    inputs: Optional[Dict[str, str]] = None,
    output: Optional[str] = None,
    **kwargs: Any,
) -> PipeDefinition:
    if name is None:
        if pipe_callable is None:
            raise
        name = make_pipe_name(pipe_callable)
    runtime_class = get_runtime_class(compatible_runtimes)
    return PipeDefinition(
        component_type=ComponentType.Pipe,
        name=name,
        module_name=module_name,
        version=version,
        pipe_callable=pipe_callable,
        compatible_runtime_classes=[runtime_class],
        declared_inputs=inputs,
        declared_output=output,
        **kwargs,
    )


def pipe(
    df_or_name: Union[str, PipeCallable] = None,
    name: str = None,
    version: str = None,
    compatible_runtimes: str = None,
    module_name: str = None,
    config_class: Optional[Type] = None,
    state_class: Optional[Type] = None,
    inputs: Optional[Dict[str, str]] = None,
    output: Optional[str] = None,
    # test_data: PipeTestCaseLike = None,
) -> Union[Callable, PipeDefinition]:
    if isinstance(df_or_name, str) or df_or_name is None:
        return partial(
            pipe,
            name=df_or_name,
            version=version,
            compatible_runtimes=compatible_runtimes,
            module_name=module_name,
            config_class=config_class,
            state_class=state_class,
            inputs=inputs,
            output=output,
        )
    return pipe_definition_factory(
        df_or_name,
        name=name,
        version=version,
        compatible_runtimes=compatible_runtimes,
        module_name=module_name,
        config_class=config_class,
        state_class=state_class,
        inputs=inputs,
        output=output,
    )


def pipe_chain(
    name: str, pipe_chain: List[Union[PipeLike, str]], **kwargs
) -> PipeDefinition:
    sub_funcs = []
    for fn in pipe_chain:
        if isinstance(fn, str):
            uri = ComponentUri.from_str(fn, component_type=ComponentType.Pipe)
        elif isinstance(fn, ComponentUri):
            uri = fn
        elif callable(fn):
            uri = make_pipe_definition(fn, **kwargs)
        else:
            raise TypeError(f"Invalid pipe uri in chain {fn}")
        sub_funcs.append(uri)
    return pipe_definition_factory(
        None, name=name, sub_graph=sub_funcs, is_composite=True, **kwargs
    )


def make_pipe_definition(dfl: PipeDefinitionLike, **kwargs) -> PipeDefinition:
    if isinstance(dfl, Pipe):
        raise TypeError(f"Already a Pipe {dfl}")
    if isinstance(dfl, PipeDefinition):
        return dfl
    return pipe_definition_factory(dfl, **kwargs)


def make_pipe(dfl: PipeLike, **kwargs) -> Pipe:
    if isinstance(dfl, Pipe):
        return dfl
    dfd = make_pipe_definition(dfl, **kwargs)
    return dfd.as_pipe()


def ensure_pipe(env: Environment, df_like: Union[PipeLike, str]) -> Pipe:
    if isinstance(df_like, Pipe):
        return df_like
    if isinstance(df_like, PipeDefinition):
        return df_like.as_pipe()
    if isinstance(df_like, str) or isinstance(df_like, ComponentUri):
        return env.get_pipe(df_like)
    return make_pipe(df_like)
