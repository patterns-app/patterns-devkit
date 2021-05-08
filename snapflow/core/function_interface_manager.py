from __future__ import annotations

from snapflow.core.declarative.base import update

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from commonmodel.base import Schema, SchemaLike, SchemaTranslation, is_any
from dcp.storage.base import Storage
from loguru import logger
from snapflow.core import operators
from snapflow.core.schema import GenericSchemaException, is_generic

if TYPE_CHECKING:
    from snapflow.core.streams import StreamBuilder
    from snapflow.core.declarative.execution import ExecutableCfg
    from snapflow.core.declarative.graph import NodeInputCfg
    from snapflow.core.environment import Environment


# def merge_declared_interface_with_signature_interface(
#     declared: DataFunctionInterface,
#     signature: DataFunctionInterface,
#     ignore_signature: bool = False,
# ) -> DataFunctionInterface:
#     # ctx can come from EITHER
#     # Take union of inputs from both, with declared taking precedence
#     # UNLESS ignore_signature, then only use signature if NO declared inputs
#     if ignore_signature and declared.inputs:
#         inputs = declared.inputs
#     else:
#         all_inputs = set(declared.inputs) | set(signature.inputs)
#         inputs = {}
#         for name in all_inputs:
#             # Declared take precedence
#             for dname, i in declared.inputs.items():
#                 if dname == name:
#                     inputs[dname] = i
#                     break
#             else:
#                 for sname, i in signature.inputs.items():
#                     if sname == name:
#                         inputs[sname] = i
#     outputs = declared.outputs or signature.outputs or {DEFAULT_OUTPUT_NAME: DEFAULT_OUTPUT} # TODO
#     return DataFunctionInterface(
#         inputs=inputs, outputs=outputs, uses_context=declared.uses_context or signature.uses_context, parameters={}
#     )


# @dataclass(frozen=True)
# class ConnectedInterface:
#     inputs: List[NodeInput]
#     interface: DataFunctionInterfaceCfg

#     @classmethod
#     def from_function_interface(
#         cls, fi: DataFunctionInterfaceCfg, declared_inputs: Dict[str, StreamInput],
#     ) -> ConnectedInterface:
#         inputs = []
#         for input in fi.inputs.values():
#             input_stream_builder = None
#             translation = None
#             assert input.name is not None
#             dni = declared_inputs.get(input.name)
#             if dni:
#                 input_stream_builder = dni.stream
#                 translation = dni.declared_schema_translation
#             ni = NodeInput(
#                 name=input.name,
#                 declared_input=input,
#                 input_stream_builder=input_stream_builder,
#                 declared_schema_translation=translation,
#             )
#             inputs.append(ni)
#         return ConnectedInterface(inputs=inputs, interface=fi)

#     def get_input(self, name: str) -> NodeInput:
#         for input in self.inputs:
#             if input.name == name:
#                 return input
#         raise KeyError(name)

#     def bind(self, input_streams: InputStreams) -> BoundInterface:
#         inputs = []
#         for node_input in self.inputs:
#             dbs = input_streams.get(node_input.name)
#             bound_stream = None
#             bound_block = None
#             if dbs is not None:
#                 bound_stream = dbs
#                 if not node_input.declared_input.is_stream:
#                     # TODO: handle StopIteration here? Happens if `get_bound_interface` is passed empty stream
#                     #   (will trigger an InputExhastedException earlier otherwise)
#                     bound_block = next(dbs)
#             si = StreamInput(
#                 name=node_input.name,
#                 declared_input=node_input.declared_input,
#                 declared_schema_translation=node_input.declared_schema_translation,
#                 input_stream_builder=node_input.input_stream_builder,
#                 is_stream=node_input.declared_input.is_stream,
#                 bound_stream=bound_stream,
#                 bound_block=bound_block,
#             )
#             inputs.append(si)
#         return BoundInterface(inputs=inputs, interface=self.interface)


# @dataclass(frozen=True)
# class StreamInput:
#     name: str
#     declared_input: DataFunctionInputCfg
#     declared_schema_translation: Optional[Dict[str, str]] = None
#     input_stream_builder: Optional[StreamBuilder] = None
#     is_stream: bool = False
#     bound_stream: Optional[DataBlockStream] = None
#     bound_block: Optional[DataBlock] = None

#     def get_bound_block_property(self, prop: str):
#         if self.bound_block:
#             return getattr(self.bound_block, prop)
#         if self.bound_stream:
#             emitted = self.bound_stream.get_emitted_managed_blocks()
#             if not emitted:
#                 # if self.bound_stream.count():
#                 #     logger.warning("No blocks emitted yet from non-empty stream")
#                 return None
#             return getattr(emitted[0], prop)
#         return None

#     def get_bound_nominal_schema(self) -> Optional[Schema]:
#         # TODO: what is this and what is this called? "resolved"?
#         return self.get_bound_block_property("nominal_schema")

#     @property
#     def nominal_schema(self) -> Optional[Schema]:
#         return self.get_bound_block_property("nominal_schema")

#     @property
#     def realized_schema(self) -> Optional[Schema]:
#         return self.get_bound_block_property("realized_schema")


# @dataclass(frozen=True)
# class BoundInterface:
#     inputs: List[StreamInput]
#     interface: DataFunctionInterfaceCfg
#     # resolved_generics: Dict[str, SchemaKey] = field(default_factory=dict)

#     def inputs_as_kwargs(self) -> Dict[str, Union[DataBlock, DataBlockStream]]:
#         return {
#             i.name: i.bound_stream if i.is_stream else i.bound_block
#             for i in self.inputs
#         }

#     def non_reference_bound_inputs(self) -> List[StreamInput]:
#         return [
#             i
#             for i in self.inputs
#             if i.bound_stream is not None and not i.declared_input.is_reference
#         ]

#     def resolve_nominal_output_schema(self, env: Environment) -> Optional[Schema]:
#         output = self.interface.get_default_output()
#         if not output:
#             return None
#         if not output.is_generic:
#             return env.get_schema(output.schema_key)
#         output_generic = output.schema_key
#         for input in self.inputs:
#             if not input.declared_input.is_generic:
#                 continue
#             if input.declared_input.schema_key == output_generic:
#                 schema = input.get_bound_nominal_schema()
#                 # We check if None -- there may be more than one input with same generic, we'll take any that are resolvable
#                 if schema is not None:
#                     return schema
#         raise Exception(f"Unable to resolve generic '{output_generic}'")


def get_schema_translation(
    env: Environment,
    source_schema: Schema,
    target_schema: Optional[Schema] = None,
    declared_schema_translation: Optional[Dict[str, str]] = None,
) -> Optional[SchemaTranslation]:
    # THE place to determine requested/necessary schema translation
    if declared_schema_translation:
        # If we are given a declared translation, then that overrides a natural translation
        return SchemaTranslation(
            translation=declared_schema_translation, from_schema_key=source_schema.key,
        )
    if target_schema is None or is_any(target_schema):
        # Nothing expected, so no translation needed
        return None
    # Otherwise map found schema to expected schema
    return source_schema.get_translation_to(target_schema)


# class NodeInterfaceManager:
#     """
#     Responsible for finding and preparing DataBlocks for input to a
#     Node.
#     """

#     def __init__(self, env: Environment, exe: ExecutableCfg):
#         self.env = env
#         self.exe = exe
#         self.node = exe.node
#         self.function_interface: DataFunctionInterfaceCfg = self.node.get_interface()

# def get_bound_interface(
#     self, input_db_streams: Optional[InputStreams] = None
# ) -> BoundInterface:
#     ci = self.get_connected_interface()
#     if input_db_streams is None:
#         input_db_streams = self.get_input_data_block_streams()
#     return ci.bind(input_db_streams)

# def get_connected_interface(self) -> ConnectedInterface:
#     inputs = self.node.get_inputs()
#     # Add "this" if it has a self-ref (TODO: a bit hidden down here no?)
#     for input in self.function_interface.inputs.values():
#         if input.is_self_reference:
#             inputs[input.name] = DeclaredStreamInput(
#                 stream=self.node.as_stream_builder(),
#                 declared_schema_translation=self.node.get_schema_translation_for_input(
#                     input.name
#                 ),
#             )
#     ci = ConnectedInterface.from_function_interface(self.function_interface, inputs)
#     return ci

# def is_input_required(self, input: DataFunctionInputCfg) -> bool:
#     # TODO: is there other logic we want here? why have method?
#     if input.required:
#         return True
#     return False


def bind_inputs(
    env: Environment, cfg: ExecutableCfg, node_inputs: Dict[str, NodeInputCfg]
) -> Dict[str, NodeInputCfg]:
    from snapflow.core.function import InputExhaustedException

    bound_inputs: Dict[str, NodeInputCfg] = {}
    any_unprocessed = False
    for node_input in node_inputs.values():
        if node_input.input_node is None:
            if not node_input.input.required:
                continue
            raise Exception(f"Missing required input {node_input.name}")
        logger.debug(
            f"Building stream for `{node_input.name}` from {node_input.input_node}"
        )
        stream_builder = node_input.as_stream_builder()
        stream_builder = _filter_stream(env, stream_builder, node_input, cfg,)

        """
        Inputs are considered "Exhausted" if:
        - Single block stream (and zero or more reference inputs): no unprocessed blocks
        - One or more reference inputs: if ALL reference streams have no unprocessed

        In other words, if ANY block stream is empty, bail out. If ALL DS streams are empty, bail
        """
        if stream_builder.get_count(env) == 0:
            logger.debug(
                f"Couldnt find eligible DataBlocks for input `{input.name}` from {stream_builder}"
            )
            if input.declared_input.required:
                raise InputExhaustedException(
                    f"    Required input '{input.name}'={stream_builder} to DataFunction '{cfg.node_key}' is empty"
                )
            continue
        else:
            bound_inputs[node_input.name] = bind_node_input(
                env, cfg, node_input, stream_builder
            )
        any_unprocessed = True

    if bound_inputs and not any_unprocessed:
        # TODO: is this really an exception always?
        logger.debug("Inputs exhausted")
        raise InputExhaustedException("All inputs exhausted")

    return bound_inputs


def _filter_stream(
    env: Environment,
    stream_builder: StreamBuilder,
    node_input: NodeInputCfg,
    cfg: ExecutableCfg,
) -> StreamBuilder:
    logger.debug(f"{stream_builder.get_count(env)} available DataBlocks")
    storages = cfg.execution_config.storages
    if storages:
        stream_builder = stream_builder.filter_storages(storages)
        logger.debug(
            f"{stream_builder.get_count(env)} available DataBlocks in storages {storages}"
        )
    if node_input.input.is_reference:
        logger.debug("Reference input, taking latest")
        stream_builder = operators.latest(stream_builder)
    else:
        logger.debug(f"Finding unprocessed input for: {stream_builder}")
        stream_builder = stream_builder.filter_unprocessed(cfg.node_key)
        logger.debug(f"{stream_builder.get_count(env)} unprocessed DataBlocks")
    return stream_builder


def bind_node_input(
    env: Environment,
    cfg: ExecutableCfg,
    node_input: NodeInputCfg,
    stream_builder: StreamBuilder,
) -> NodeInputCfg:
    bound_block = None
    bound_stream = None
    if stream_builder is not None:
        schema = None
        try:
            schema = env.get_schema(node_input.input.schema_key)
        except GenericSchemaException:
            pass
        bound_stream = stream_builder.as_managed_stream(
            env,
            cfg.execution_config,
            declared_schema=schema,
            declared_schema_translation=node_input.schema_translation,
        )
        if not node_input.input.is_stream:
            # TODO: handle StopIteration here? Happens if `get_bound_interface` is passed empty stream
            #   (will trigger an InputExhastedException earlier otherwise)
            bound_block = next(bound_stream)
    return update(node_input, bound_block=bound_block, bound_stream=bound_stream)

