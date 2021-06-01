from __future__ import annotations

from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import networkx as nx
from attr import validate
from commonmodel import Schema
from dcp.utils.common import as_identifier, remove_dupes
from pydantic import validator
from pydantic.class_validators import root_validator
from snapflow.core.component import ComponentLibrary, global_library
from snapflow.core.declarative.base import FrozenPydanticBase
from snapflow.core.declarative.function import (
    DataFunctionCfg,
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
)

if TYPE_CHECKING:
    from snapflow.core.data_block import DataBlock
    from snapflow.core.streams import StreamBuilder, DataBlockStream
    from snapflow.core.declarative.flow import FlowCfg
    from snapflow.core.declarative.execution import NodeInputCfg


NxNode = Tuple[str, Dict[str, Dict]]
NxAdjacencyList = List[NxNode]


def startswith_any(s: str, others: Iterable[str]) -> bool:
    for o in others:
        if not o:
            continue
        if s.startswith(o):
            return True
    return False


class DedupeBehavior(str, Enum):
    NONE = "None"
    LATEST_RECORD = "LatestRecord"
    FIRST_NON_NULL_VALUES = "FirstNonNullValues"
    LATEST_NON_NULL_VALUES = "LatestNonNullValues"


class GraphCfg(FrozenPydanticBase):
    key: str = "default"
    nodes: List[GraphCfg] = []
    function: Optional[str] = None
    function_cfg: Optional[DataFunctionCfg] = None
    flow: Optional[str] = None
    params: Dict[str, Any] = {}  # TODO: acceptable param types?
    stdin_key: Optional[str] = None
    stdout_key: Optional[str] = None
    stderr_key: Optional[str] = None
    input: Optional[str] = None
    inputs: Dict[str, str] = {}
    alias: Optional[str] = None
    aliases: Dict[str, str] = {}
    accumulate: Optional[bool] = False
    dedupe: Optional[Union[bool, DedupeBehavior]] = False
    conform_to_schema: Optional[str] = None
    schema_translation: Optional[Dict[str, str]] = None
    schema_translations: Dict[str, Dict[str, str]] = {}

    @validator("nodes")
    def check_unique_nodes(cls, nodes: List[GraphCfg]) -> List[GraphCfg]:
        assert len(nodes) == len(set(n.key for n in nodes)), "Node keys must be unique"
        return nodes

    @root_validator
    def check_node_keys(cls, values: Dict) -> Dict:
        nodes = values.get("nodes", [])
        node_keys = [n.key for n in nodes if n.key]
        key = values.get("key")
        stdin = values.get("stdin_key")
        stdout = values.get("stdout_key")
        if not nodes:
            assert not stdin or startswith_any(
                stdin, [key or "_NONE_", "stdin"]
            ), f"stdin_key '{stdin}' does not exist"
            assert not stdout or startswith_any(
                stdout, [key or "_NONE_", "stdout"]
            ), f"stdout_key '{stdout}' does not exist"
        else:
            assert not stdin or startswith_any(
                stdin, [key or "_NONE_", "stdin"] + node_keys
            ), f"stdin_key '{stdin}' does not exist"
            assert not stdout or startswith_any(
                stdout, [key or "_NONE_", "stdout"] + node_keys
            ), f"stdout_key '{stdout}' does not exist"
        for n in nodes:
            for inpt_node in n.get_inputs().values():
                assert inpt_node == "stdout" or startswith_any(
                    inpt_node, node_keys
                ), f"input node key '{inpt_node}' does not exist"
        return values

    @root_validator
    def check_single_purpose(cls, values: Dict) -> Dict:
        vals = [
            p
            for p in [values.get("function"), values.get("flow"), values.get("nodes")]
            if p
        ]
        assert 1 >= len(
            vals
        ), f"Every graph config is at most one of: single function, flow definition, or sub-graph: {vals}"
        return values

    def resolve(self, lib: Optional[ComponentLibrary] = None) -> GraphCfg:
        if self.is_resolved():
            return self
        if lib is None:
            lib = global_library
        d = self.dict()
        nodes = self.nodes
        if self.flow:
            assert not self.nodes
            flow: FlowCfg = lib.get_flow(self.flow)
            fg = flow.graph
            nodes = fg.nodes
            # Update flow defaults with this graphs settings
            flow_dict = fg.dict()
            flow_dict.update({k: v for k, v in d.items() if v is not None})
            flow_dict["flow"] = None  # Flow has been resolved, remove
            d = flow_dict
        d["nodes"] = [n.resolve(lib) for n in nodes]
        if self.function:
            d["function_cfg"] = lib.get_function(self.function).to_config()
        return GraphCfg(**d)

    def flatten(self) -> GraphCfg:
        from snapflow.core.flattener import flatten_graph_config

        return flatten_graph_config(self)

    def resolve_and_flatten(self, lib: ComponentLibrary) -> GraphCfg:
        return self.resolve(lib).flatten()

    def is_function_node(self) -> bool:
        return (
            (self.function is not None or self.function_cfg is not None)
            and not self.nodes
            and not self.flow
        )

    def has_augmentations(self) -> bool:
        if self.accumulate:
            return True
        if self.dedupe:
            return True
        if self.conform_to_schema:
            return True
        return False

    def is_flattened(self) -> bool:
        if self.has_augmentations():
            return False
        if not self.nodes:
            return True
        for n in self.nodes:
            if n.nodes or n.has_augmentations():
                return False
        return True

    def is_resolved(self) -> bool:
        if self.flow and not self.nodes:
            return False
        if self.function and not self.function_cfg:
            return False
        return all([n.is_resolved() for n in self.nodes])

    def get_stdin_key(self) -> str:
        if self.stdin_key:
            return self.stdin_key
        if not self.nodes:
            return self.key
        raise Exception(
            f"Must specify 'stdin_key' when graph has multiple possible input nodes: {self}"
        )

    def get_stdout_key(self) -> str:
        if self.stdout_key:
            return self.stdout_key
        if not self.nodes:
            return self.key
        raise Exception(
            f"Must specify 'stdout_key' when graph has multiple possible output nodes: {self}"
        )

    def get_inputs(self) -> Dict[str, str]:
        if self.input:
            return {"stdin": self.input}
        return self.inputs

    def get_aliases(self) -> Dict[str, str]:
        if self.alias:
            return {"stdout": self.alias}
        return self.aliases

    def get_schema_translations(self) -> Dict[str, Dict[str, str]]:
        if self.schema_translation:
            return {"stdin": self.schema_translation}
        return self.schema_translations

    def node_dict(self) -> Dict[str, GraphCfg]:
        return {n.key: n for n in self.nodes if n.key}

    def add(self, g: GraphCfg):
        if g.key in self.node_dict():
            raise KeyError(
                f"Duplicate node key `{g.key}`. Specify a distinct key for the node/graph"
            )
        self.nodes.append(g)

    # def remove(self, node: DeclaredNode):
    #     del self._nodes[node.key]

    def get_node(self, key: Union[GraphCfg, str]) -> GraphCfg:
        if isinstance(key, GraphCfg):
            return key
        assert isinstance(key, str)
        return self.node_dict()[key]

    def as_nx_graph(self) -> nx.DiGraph:
        g = nx.DiGraph()
        for n in self.nodes:
            g.add_node(n.key)
            inputs = n.get_inputs()
            for input_node_key in inputs.values():
                g.add_node(input_node_key)
                g.add_edge(input_node_key, n.key)
        return g

    def adjacency_list(self) -> NxAdjacencyList:
        return list(self.as_nx_graph().adjacency())

    def get_all_upstream_dependencies_in_execution_order(
        self, node: GraphCfg
    ) -> List[GraphCfg]:
        g = self.as_nx_graph()
        node_keys = self._get_all_upstream_dependencies_in_execution_order(g, node.key)
        return [self.get_node(name) for name in node_keys]

    def _get_all_upstream_dependencies_in_execution_order(
        self, g: nx.DiGraph, node: str
    ) -> List[str]:
        nodes = []
        for parent_node in g.predecessors(node):
            if parent_node == node:
                # Ignore self-ref cycles
                continue
            parent_deps = self._get_all_upstream_dependencies_in_execution_order(
                g, parent_node
            )
            nodes.extend(parent_deps)
        nodes.append(node)
        # May have added nodes twice, just keep first reference:
        return remove_dupes(nodes)

    def get_all_nodes_in_execution_order(self) -> List[GraphCfg]:
        g = self.as_nx_graph()
        return [self.get_node(name) for name in nx.topological_sort(g)]

    def get_alias(self, node_key: Optional[str] = None) -> str:
        ident = None
        aliases = self.get_aliases()
        if aliases:
            ident = aliases.get(node_key or "stdout")
        if ident is None:
            ident = self.key
        return as_identifier(
            ident
        )  # TODO: this logic should be storage api specific! and then shared back?

    def get_interface(self) -> DataFunctionInterfaceCfg:
        assert self.is_function_node()  # TODO: can actually support for graph too!
        assert self.is_resolved()
        assert self.function_cfg.interface is not None
        return self.function_cfg.interface

    def get_node_inputs(self, graph: GraphCfg) -> Dict[str, NodeInputCfg]:
        from snapflow.core.declarative.execution import NodeInputCfg

        assert self.is_function_node()  # TODO: can actually support for graph too!
        assert self.is_resolved()
        declared_inputs = self.assign_inputs()
        node_inputs = {}
        for inpt in self.function_cfg.interface.inputs.values():
            declared_input = declared_inputs.get(inpt.name)
            input_node = None
            if declared_input:
                input_node = graph.get_node(declared_input)
            if input_node is None and inpt.is_self_reference:
                input_node = self
            node_inputs[inpt.name] = NodeInputCfg(
                name=inpt.name,
                input=inpt,
                input_node=input_node,
                schema_translation=self.get_schema_translations().get(
                    inpt.name
                ),  # TODO: doesn't handle stdin
            )
        return node_inputs

    def assign_inputs(self) -> Dict[str, str]:
        assert self.is_function_node()  # TODO: can actually support for graph too!
        assert self.is_resolved()
        inputs = self.get_inputs()
        interface = self.function_cfg.interface
        if "stdin" in inputs:
            stdin_name = interface.get_stdin_name()
            assert stdin_name is not None, f"No stdin on interface {interface}"
            inputs[stdin_name] = inputs.pop("stdin")
        input_names_have = set(inputs.keys())
        input_names_must_have = set(
            i.name for i in interface.inputs.values() if i.required
        )
        input_names_ok_to_have = set(interface.inputs)
        assert (
            input_names_have >= input_names_must_have
        ), f"Missing required input(s): {input_names_must_have - input_names_have}"
        assert (
            input_names_have <= input_names_ok_to_have
        ), f"Extra input(s): {input_names_have - input_names_ok_to_have}"
        return inputs

    def as_stream_builder(self) -> StreamBuilder:
        from snapflow.core.streams import StreamBuilder

        return StreamBuilder().filter_inputs([self.key])

    # def has_node(self, key: str) -> bool:
    #     return key in self._nodes

    # def all_nodes(self) -> Iterable[DeclaredNode]:
    #     return self._nodes.values()

    # def instantiate(self, env: Environment) -> Graph:
    #     g = Graph(env)
    #     for dn in self.all_nodes():
    #         n = dn.instantiate(env, g)
    #         g.add_node(n)
    #     return g


GraphCfg.update_forward_refs()
