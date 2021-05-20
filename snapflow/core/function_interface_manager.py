from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from commonmodel.base import Schema, SchemaLike, SchemaTranslation, is_any
from dcp.storage.base import Storage
from loguru import logger
from snapflow.core import operators
from snapflow.core.data_block import DataBlock
from snapflow.core.declarative.base import FrozenPydanticBase
from snapflow.core.declarative.execution import ExecutableCfg, NodeInputCfg
from snapflow.core.declarative.function import (
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
)
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment
from snapflow.core.schema import GenericSchemaException, is_generic
from snapflow.core.streams import DataBlockStream, StreamBuilder


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
            translation=declared_schema_translation,
            from_schema_key=source_schema.key,
        )
    if target_schema is None or is_any(target_schema):
        # Nothing expected, so no translation needed
        return None
    # Otherwise map found schema to expected schema
    return source_schema.get_translation_to(target_schema)


@dataclass(frozen=True)
class BoundInput:
    name: str
    input: DataFunctionInputCfg
    input_node: Optional[GraphCfg] = None
    schema_translation: Optional[Dict[str, str]] = None
    bound_stream: Optional[DataBlockStream] = None
    bound_block: Optional[DataBlock] = None

    def is_bound(self) -> bool:
        return self.bound_stream is not None or self.bound_block is not None

    def get_bound_block_property(self, prop: str):
        if self.bound_block:
            return getattr(self.bound_block, prop)
        if self.bound_stream:
            emitted = self.bound_stream.get_emitted_managed_blocks()
            if not emitted:
                # if self.bound_stream.count():
                #     logger.warning("No blocks emitted yet from non-empty stream")
                return None
            return getattr(emitted[0], prop)
        return None

    def get_bound_nominal_schema(self) -> Optional[Schema]:
        return self.get_bound_block_property("nominal_schema")

    def get_bound_realized_schema(self) -> Optional[Schema]:
        return self.get_bound_block_property("nominal_schema")

    @property
    def nominal_schema(self) -> Optional[Schema]:
        return self.get_bound_nominal_schema()

    @property
    def realized_schema(self) -> Optional[Schema]:
        return self.get_bound_realized_schema()

    @property
    def is_stream(self) -> bool:
        return self.input.is_stream


@dataclass(frozen=True)
class BoundInterface:
    inputs: Dict[str, BoundInput]
    interface: DataFunctionInterfaceCfg

    def inputs_as_kwargs(self) -> Dict[str, Union[DataBlock, DataBlockStream]]:
        assert all([i.is_bound() for i in self.inputs.values()])
        return {
            i.name: i.bound_stream if i.is_stream else i.bound_block
            for i in self.inputs.values()
        }

    def non_reference_bound_inputs(self) -> List[NodeInputCfg]:
        return [
            i
            for i in self.inputs.values()
            if i.bound_stream is not None and not i.input.is_reference
        ]

    def resolve_nominal_output_schema(self) -> Optional[str]:
        output = self.interface.get_default_output()
        if not output:
            return None
        if not output.is_generic:
            return output.schema_key
        output_generic = output.schema_key
        for node_input in self.inputs.values():
            if not node_input.input.is_generic:
                continue
            if node_input.input.schema_key == output_generic:
                schema = node_input.get_bound_nominal_schema()
                # We check if None -- there may be more than one input with same generic, we'll take any that are resolvable
                if schema is not None:
                    return schema.key
        raise Exception(f"Unable to resolve generic '{output_generic}'")


def bind_inputs(
    env: Environment, cfg: ExecutableCfg, node_inputs: Dict[str, NodeInputCfg]
) -> Dict[str, BoundInput]:
    from snapflow.core.function import InputExhaustedException

    bound_inputs: Dict[str, BoundInput] = {}
    any_unprocessed = False
    for node_input in node_inputs.values():
        if node_input.input_node is None:
            if not node_input.input.required:
                # bound_inputs[node_input.name] = node_input.as_bound_input()
                continue
            raise Exception(f"Missing required input {node_input.name}")
        logger.debug(
            f"Building stream for `{node_input.name}` from {node_input.input_node}"
        )
        stream_builder = node_input.as_stream_builder()
        stream_builder = _filter_stream(
            env,
            stream_builder,
            node_input,
            cfg,
        )

        """
        Inputs are considered "Exhausted" if:
        - Single block stream (and zero or more reference inputs): no unprocessed blocks
        - One or more reference inputs: if ALL reference streams have no unprocessed

        In other words, if ANY block stream is empty, bail out. If ALL DS streams are empty, bail
        """
        if stream_builder.get_count(env) == 0:
            logger.debug(
                f"Couldnt find eligible DataBlocks for input `{node_input.name}` from {stream_builder}"
            )
            if node_input.input.required:
                raise InputExhaustedException(
                    f"    Required input '{node_input.name}'={stream_builder} to DataFunction '{cfg.node_key}' is empty"
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
) -> BoundInput:
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
    return node_input.as_bound_input(
        bound_block=bound_block,
        bound_stream=bound_stream,
    )
