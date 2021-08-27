from dataclasses import dataclass
from typing import Dict, List

from basis.core import environment
from basis.core.component import ComponentLibrary
from basis.core.declarative.base import PydanticBase
from basis.core.declarative.environment import ComponentLibraryCfg
from basis.core.declarative.execution import (
    ExecutableCfg,
    ExecutionCfg,
    ExecutionResult,
)
from basis.core.graph import Graph, instantiate_graph
from basis.core.node import Node, instantiate_node
from basis.core.persistence.pydantic import BlockMetadataCfg, BlockWithStoredBlocksCfg


@dataclass
class Executable:
    node: Node
    graph: Graph
    execution_cfg: ExecutionCfg
    # bound_interface: BoundInterfaceCfg
    # results: ExecutionResult
    library: ComponentLibrary
    original_cfg: ExecutableCfg
    input_blocks: Dict[str, List[BlockWithStoredBlocksCfg]]


def instantiate_library(cfg: ComponentLibraryCfg) -> ComponentLibrary:
    from basis.core.function_package import load_function_from_source_file

    lib = ComponentLibrary.from_config(cfg)
    # lib.merge(global_library)
    for src in cfg.source_file_functions:
        fn = load_function_from_source_file(src)
        lib.add_function(fn)
    return lib


def instantiate_executable(cfg: ExecutableCfg) -> Executable:
    if cfg.library_cfg is not None:
        lib = instantiate_library(cfg.library_cfg)
    else:
        lib = ComponentLibrary()
    graph = instantiate_graph(cfg.graph, lib)
    return Executable(
        node=graph.get_node(cfg.node_key),
        graph=graph,
        execution_cfg=cfg.execution_cfg,
        input_blocks=cfg.input_blocks,
        # result=cfg.result,
        library=lib,
        original_cfg=cfg,
    )
