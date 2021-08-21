from basis.core.declarative.base import PydanticBase
from basis.core.declarative.environment import ComponentLibraryCfg
from basis.core.component import ComponentLibrary
from basis.core.node import Node, instantiate_node
from basis.core.graph import Graph, instantiate_graph
from basis.core.declarative.execution import (
    ExecutableCfg,
    ExecutionCfg,
    ExecutionResult,
)


class Executable(PydanticBase):
    node: Node
    graph: Graph
    execution_config: ExecutionCfg
    bound_interface: BoundInterfaceCfg
    # results: ExecutionResult
    library: ComponentLibrary
    original_cfg: ExecutableCfg


def instantiate_library(cfg: ComponentLibraryCfg) -> ComponentLibrary:
    from basis.core.function_package import load_function_from_source_file

    lib = ComponentLibrary.from_config(cfg)
    lib.merge(global_library)
    for src in cfg.source_file_functions:
        fn = load_function_from_source_file(src)
        lib.add_function(fn)
    return lib


def instantiate_executable(cfg: ExecutableCfg) -> Executable:
    lib = ComponentLibrary()
    graph = instantiate_graph(cfg.node_set, lib)
    return Executable(
        node=graph.get_node(cfg.node_key),
        graph=graph,
        execution_config=cfg.execution_config,
        # result=cfg.result,
        library=lib,
        boudn_interface=cfg.bound_interface,
        original_config=cfg,
    )
