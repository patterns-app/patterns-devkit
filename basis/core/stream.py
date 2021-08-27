from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from basis.core.declarative.interface import NodeInputCfg
from basis.core.declarative.node import NodeCfg
from basis.core.environment import Environment
from basis.core.graph import Graph
from basis.core.node import Node, instantiate_node
from basis.core.persistence.block import BlockMetadata, StoredBlockMetadata
from basis.core.persistence.pydantic import BlockWithStoredBlocksCfg
from basis.core.persistence.state import Direction, ExecutionLog, StreamState
from commonmodel.base import Schema, SchemaLike, SchemaTranslation, is_any
from dcp.storage.base import Storage
from loguru import logger
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.elements import not_
from sqlalchemy.sql.expression import and_, select
from sqlalchemy.sql.selectable import Select

if TYPE_CHECKING:
    from basis.core.declarative.execution import ExecutionCfg


def get_schema_translation(
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


def get_input_blocks(
    env: Environment, cfg: ExecutionCfg, node: Node, graph: Graph
) -> Dict[str, List[BlockWithStoredBlocksCfg]]:
    node_inputs = graph.get_node_inputs(node)
    return bind_inputs(env, cfg, node, node_inputs)


# def get_bound_interface(
#     env: Environment, cfg: ExecutionCfg, node: GraphCfg, graph: GraphCfg
# ) -> Any:
#     node_inputs = graph.get_node_inputs(instantiate_node(node))
#     bound_inputs = bind_inputs(env, cfg, node, node_inputs)
#     return BoundInterfaceCfg(inputs=bound_inputs, interface=node.get_interface(),)


def bind_inputs(
    env: Environment,
    cfg: ExecutionCfg,
    node: Node,
    node_inputs: Dict[str, NodeInputCfg],
) -> Dict[str, List[BlockWithStoredBlocksCfg]]:
    from basis.core.function import InputExhaustedException

    bound_inputs: Dict[str, List[BlockWithStoredBlocksCfg]] = {}
    any_unprocessed = False
    for node_input in node_inputs.values():
        if node_input.input_node is None:
            if not node_input.input.required:
                continue
            raise Exception(f"Missing required input {node_input.name}")
        logger.debug(
            f"Building stream for '{node_input.name}' from '{node_input.input_node.key}'"
        )
        block_stream_query = _filter_blocks(env, node, node_input, cfg)
        block_stream: List[BlockMetadata] = list(
            env.md_api.execute(block_stream_query).scalars()
        )

        """
        Inputs are considered "Exhausted" if:
        - Single block stream (and zero or more reference inputs): no unprocessed blocks
        - One or more reference inputs: if ALL reference streams have no unprocessed

        In other words, if ANY block stream is empty, bail out. If ALL DS streams are empty, bail
        """
        if len(block_stream) == 0:
            logger.debug(
                f"Couldnt find eligible Blocks for input '{node_input.name}' from node '{node_input.input_node.key}'"
            )
            if node_input.input.required:
                raise InputExhaustedException(
                    f"    Required input '{node_input.name}' (from node '{node_input.input_node.key}') to node '{node.key}' is empty"
                )
            continue
        else:
            bound_inputs[node_input.name] = [
                db.to_pydantic_with_stored() for db in block_stream
            ]
        any_unprocessed = True

    if bound_inputs and not any_unprocessed:
        # TODO: is this really an exception always?
        logger.debug("Inputs exhausted")
        raise InputExhaustedException("All inputs exhausted")

    return bound_inputs


def _filter_blocks(
    env: Environment,
    node: Node,
    node_input: NodeInputCfg,
    cfg: ExecutionCfg,
) -> Select:
    node = node
    eligible_input_dbs = (
        select(BlockMetadata)
        .join(StreamState)
        .filter(
            StreamState.node_key == node_input.input_node.key,
            StreamState.stream_name == node_input.input_stream,
            StreamState.latest_processed_block_id == BlockMetadata.id,
        )
    )
    return eligible_input_dbs

    # logger.opt(lazy=True).debug(
    #     "{x} all Blocks", x=lambda: env.md_api.count(eligible_input_dbs)
    # )
    # eligible_input_logs = (
    #     Query(BlockLog.block_id)
    #     .join(ExecutionLog)
    #     .filter(
    #         BlockLog.direction == Direction.OUTPUT,
    #         # BlockLog.stream_name == stream_name,
    #         ExecutionLog.node_key == node_input.input_node.key,
    #     )
    #     .filter(BlockLog.invalidated == False)  # noqa
    #     .distinct()
    # )
    # eligible_input_dbs = eligible_input_dbs.filter(
    #     BlockMetadata.id.in_(eligible_input_logs)
    # )
    # logger.opt(lazy=True).debug(
    #     "{x} available Blocks", x=lambda: env.md_api.count(eligible_input_dbs)
    # )
    # storages = cfg.storages
    # if storages:
    #     eligible_input_dbs = eligible_input_dbs.join(StoredBlockMetadata).filter(
    #         StoredBlockMetadata.storage_url.in_(storages)  # type: ignore
    #     )
    #     logger.opt(lazy=True).debug(
    #         "{x} available Blocks in storages {storages}",
    #         x=lambda: env.md_api.count(eligible_input_dbs),
    #         storages=lambda: storages,
    #     )
    # if node_input.input.is_reference:
    #     logger.debug("Reference input, taking latest")
    #     eligible_input_dbs = eligible_input_dbs.order_by(BlockMetadata.id.desc()).limit(
    #         1
    #     )
    # else:
    #     eligible_input_dbs = eligible_input_dbs.order_by(BlockMetadata.id)
    #     eligible_input_dbs = _filter_unprocessed(eligible_input_dbs, node.key)
    #     logger.opt(lazy=True).debug(
    #         "{x} unprocessed Blocks", x=lambda: env.md_api.count(eligible_input_dbs)
    #     )


# def _filter_unprocessed(query: Select, unprocessed_by_node_key: str) -> Select:
#     filter_clause = and_(
#         BlockLog.direction == Direction.INPUT,
#         ExecutionLog.node_key == unprocessed_by_node_key,
#     )
#     # else:
#     #     # No block cycles allowed
#     #     # Exclude blocks processed as INPUT and blocks outputted
#     #     filter_clause = (
#     #         ExecutionLog.node_key == self._filters.unprocessed_by_node_key
#     #     )
#     already_processed_drs = (
#         Query(BlockLog.block_id)
#         .join(ExecutionLog)
#         .filter(filter_clause)
#         .filter(BlockLog.invalidated == False)  # noqa
#         .distinct()
#     )
#     return query.filter(not_(BlockMetadata.id.in_(already_processed_drs)))


def get_all_schema_keys(inputs: Dict[str, List[BlockWithStoredBlocksCfg]]) -> List[str]:
    schemas = []
    for blocks in inputs.values():
        for b in blocks:
            if b.nominal_schema_key:
                schemas.append(b.nominal_schema_key)
            if b.realized_schema_key:
                schemas.append(b.realized_schema_key)
            if b.inferred_schema_key:
                schemas.append(b.inferred_schema_key)
    return schemas
