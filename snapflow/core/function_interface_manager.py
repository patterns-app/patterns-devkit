from __future__ import annotations

from dataclasses import dataclass
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment

from sqlalchemy.sql.selectable import Select
from snapflow.core.declarative.execution import ExecutionCfg

from sqlalchemy.sql.expression import select
from snapflow.core.persisted.data_block import (
    DataBlockMetadata,
    StoredDataBlockMetadata,
)
from snapflow.core.persisted.state import DataBlockLog, DataFunctionLog, Direction

from sqlalchemy.orm.query import Query
from snapflow.core.declarative.interface import (
    BoundInputCfg,
    BoundInterfaceCfg,
    NodeInputCfg,
)
from typing import Iterable, TYPE_CHECKING, Any, Dict, List, Optional, Union

from commonmodel.base import Schema, SchemaLike, SchemaTranslation, is_any
from dcp.storage.base import Storage
from loguru import logger


def get_schema_translation(
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


def get_bound_interface(
    env: Environment, cfg: ExecutionCfg, node_key: str, graph: GraphCfg
) -> BoundInterfaceCfg:
    node = graph.get_node(node_key)
    node_inputs = node.get_node_inputs(graph)
    bound_inputs = bind_inputs(env, cfg, node, node_inputs)
    return BoundInterfaceCfg(inputs=bound_inputs, interface=node.get_interface(),)


def bind_inputs(
    env: Environment,
    cfg: ExecutionCfg,
    node: GraphCfg,
    node_inputs: Dict[str, NodeInputCfg],
) -> Dict[str, BoundInputCfg]:
    from snapflow.core.function import InputExhaustedException

    bound_inputs: Dict[str, BoundInputCfg] = {}
    any_unprocessed = False
    for node_input in node_inputs.values():
        if node_input.input_node is None:
            if not node_input.input.required:
                continue
            raise Exception(f"Missing required input {node_input.name}")
        logger.debug(
            f"Building stream for `{node_input.name}` from {node_input.input_node}"
        )
        block_stream_query = _filter_blocks(node, node_input, cfg)
        block_stream: List[DataBlockMetadata] = list(
            env.md_api.execute(block_stream_query)
        )

        """
        Inputs are considered "Exhausted" if:
        - Single block stream (and zero or more reference inputs): no unprocessed blocks
        - One or more reference inputs: if ALL reference streams have no unprocessed

        In other words, if ANY block stream is empty, bail out. If ALL DS streams are empty, bail
        """
        if len(block_stream) == 0:
            logger.debug(
                f"Couldnt find eligible DataBlocks for input `{node_input.name}` from node {node_input.input_node.key}"
            )
            if node_input.input.required:
                raise InputExhaustedException(
                    f"    Required input '{node_input.name}' (from node '{node_input.input_node.key}') to node '{node.key}' is empty"
                )
            continue
        else:
            bound_inputs[node_input.name] = node_input.as_bound_input(
                bound_stream=[db.to_pydantic_with_stored() for db in block_stream]
            )
        any_unprocessed = True

    if bound_inputs and not any_unprocessed:
        # TODO: is this really an exception always?
        logger.debug("Inputs exhausted")
        raise InputExhaustedException("All inputs exhausted")

    return bound_inputs


def _filter_blocks(
    node: GraphCfg, node_input: NodeInputCfg, cfg: ExecutionCfg,
) -> Select:
    node = node
    eligible_input_dbs = select(DataBlockMetadata)
    eligible_input_logs = (
        Query(DataBlockLog.data_block_id)
        .join(DataFunctionLog)
        .filter(
            DataBlockLog.direction == Direction.OUTPUT,
            # DataBlockLog.stream_name == stream_name,
            DataFunctionLog.node_key == node.key,
        )
        .filter(DataBlockLog.invalidated == False)  # noqa
        .distinct()
    )
    eligible_input_dbs = eligible_input_dbs.filter(
        DataBlockMetadata.id.in_(eligible_input_logs)
    )
    logger.opt(lazy=True).debug(
        "{x} available DataBlocks", x=lambda: eligible_input_dbs.count()
    )
    storages = cfg.storages
    if storages:
        eligible_input_dbs = eligible_input_dbs.join(StoredDataBlockMetadata).filter(
            StoredDataBlockMetadata.storage_url.in_(storages)  # type: ignore
        )
        logger.opt(lazy=True).debug(
            "{x} available DataBlocks in storages {storages}",
            x=lambda: eligible_input_dbs.count(),
            storages=storages,
        )
    if node_input.input.is_reference:
        logger.debug("Reference input, taking latest")
        eligible_input_dbs = eligible_input_dbs.order_by(
            DataBlockMetadata.id.desc()
        ).limit(1)
    else:
        eligible_input_dbs = eligible_input_dbs.order_by(DataBlockMetadata.id)
        eligible_input_dbs = eligible_input_dbs.filter_unprocessed(node.key)
        logger.opt(lazy=True).debug(
            "{x} unprocessed DataBlocks", x=lambda: eligible_input_dbs.count()
        )
    return eligible_input_dbs.all()

