import textwrap
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Any, Union, List, Dict, Collection, Set
from basis.graph.builder import graph_manifest_from_yaml, GraphManifest

from commonmodel import Schema

from basis.configuration.path import GraphEdge, NodeId, PortId
from basis.graph.configured_node import (
    ParameterDefinition,
    ParameterType,
    OutputDefinition,
    PortType,
    InputDefinition,
    ConfiguredNode,
    NodeType,
    NodeInterface,
    GraphManifest,
    GraphError,
    StateDefinition,
)


class _IgnoreType:
    def __eq__(self, other):
        return isinstance(other, _IgnoreType)

    def __repr__(self):
        return "IGNORE"


IGNORE = _IgnoreType()


@dataclass
class NodeAssertion:
    name: Union[str, _IgnoreType]
    node_type: Union[NodeType, _IgnoreType]
    id: Union[NodeId, _IgnoreType]
    parent_node_id: Union[str, None]
    interface: Union[NodeInterface, _IgnoreType]
    description: Union[str, _IgnoreType]
    file_path_to_node_script_relative_to_root: Union[str, _IgnoreType]
    parameter_values: Union[Dict[str, Any], _IgnoreType]
    schedule: Union[str, _IgnoreType]
    local_edges: Union[List[str], _IgnoreType]
    resolved_edges: Union[List[str], _IgnoreType]
    errors: List[str]


def _calculate_graph(nodes: List[ConfiguredNode]):
    nodes_by_id = {n.id: n for n in nodes}
    ids_by_path = {None: None}

    for node in nodes:
        parts = [node.name]
        parent = node.parent_node_id
        while parent:
            n = nodes_by_id[parent]
            parts.append(n.name)
            parent = n.parent_node_id
        ids_by_path[".".join(reversed(parts))] = node.id

    return ids_by_path


def _assert_node(
    node: ConfiguredNode, assertion: NodeAssertion, ids_by_path: dict, paths_by_id: dict
):
    for k, v in asdict(assertion).items():
        if k == "errors":
            continue
        actual = getattr(node, k)
        if k == "id":
            assert actual, "all ids must be defined"
        if v == IGNORE:
            continue

        if k in ("local_edges", "resolved_edges"):
            v = _unordered([_ge(s, ids_by_path) for s in v])
            actual = _unordered(actual)
        elif k == "parent_node_id" and v is not None:
            actual = paths_by_id[actual]

        assert (
            actual == v
        ), f"{k}: expected: {_tostr(v, paths_by_id)} but was: {_tostr(actual, paths_by_id)}"


def _tostr(it, paths_by_id: dict) -> str:
    if isinstance(it, (list, set)):
        return str(sorted(_tostr(i, paths_by_id) for i in it))
    if isinstance(it, PortId):
        return f"{paths_by_id[it.node_id]}:{it.port}"
    if isinstance(it, GraphEdge):
        return f"{_tostr(it.input, paths_by_id)} -> {_tostr(it.output, paths_by_id)}"
    if isinstance(it, GraphError):
        return f"{paths_by_id[it.node_id] if it.node_id else '<root>'}: {it.message}"
    return repr(it)


def assert_nodes(
    manifest: GraphManifest, *expected: NodeAssertion, assert_length: bool = True
):
    nodes = manifest.nodes
    assert len(nodes) == len({node.id for node in nodes}), "all ids must be unique"

    if assert_length:
        assert len(nodes) == len(
            expected
        ), f"expected nodes {[n.name for n in expected]} , got {[n.name for n in nodes]}"

    nodes_by_name_and_parent = {
        (node.name, node.parent_node_id): node for node in nodes
    }
    ids_by_path = _calculate_graph(nodes)
    paths_by_id = {v: k for (k, v) in ids_by_path.items()}
    errors = []
    for assertion in expected:
        parent_id = ids_by_path[assertion.parent_node_id]
        node = nodes_by_name_and_parent[assertion.name, parent_id]
        errors.extend(GraphError(node_id=node.id, message=e) for e in assertion.errors)

    ac = _unordered(manifest.errors)
    ex = _unordered(errors)
    assert (
        ex == ac
    ), f"errors:\nexpected:\n{_tostr(ex, paths_by_id)}\nbut was:\n{_tostr(ac, paths_by_id)}"

    for assertion in expected:
        parent_id = ids_by_path[assertion.parent_node_id]
        node = nodes_by_name_and_parent[assertion.name, parent_id]
        _assert_node(node, assertion, ids_by_path, paths_by_id)


# noinspection PyDefaultArgument
def n(
    name: str,
    node_type: Union[NodeType, _IgnoreType] = NodeType.Node,
    id: Union[str, _IgnoreType] = IGNORE,
    parent: Union[str, None] = None,
    interface: Union[
        List[
            Union[
                InputDefinition, OutputDefinition, ParameterDefinition, StateDefinition
            ]
        ],
        _IgnoreType,
    ] = IGNORE,
    description: Union[str, None, _IgnoreType] = None,
    file_path: Union[str, _IgnoreType] = IGNORE,
    parameter_values: Union[Dict[str, Any], _IgnoreType] = {},
    schedule: Union[str, None, _IgnoreType] = None,
    local_edges: Union[List[str], _IgnoreType] = IGNORE,
    resolved_edges: Union[List[str], None, _IgnoreType] = None,
    errors: List[str] = [],
) -> NodeAssertion:
    return NodeAssertion(
        name=name,
        node_type=node_type,
        id=IGNORE if id == IGNORE else NodeId(id.lower()),
        parent_node_id=parent,
        interface=interface
        if interface == IGNORE
        else NodeInterface(
            inputs=[i for i in interface if isinstance(i, InputDefinition)],
            outputs=[i for i in interface if isinstance(i, OutputDefinition)],
            parameters=[i for i in interface if isinstance(i, ParameterDefinition)],
            state=next((i for i in interface if isinstance(i, StateDefinition)), None),
        ),
        description=description,
        file_path_to_node_script_relative_to_root=file_path,
        parameter_values=parameter_values,
        schedule=schedule,
        local_edges=local_edges,
        resolved_edges=local_edges if resolved_edges is None else resolved_edges,
        errors=errors,
    )


def p(
    name: str, parameter_type: str = None, description: str = None, default: Any = None,
) -> ParameterDefinition:
    return ParameterDefinition(
        name=name,
        parameter_type=ParameterType(parameter_type) if parameter_type else None,
        description=description,
        default=default,
    )


def ostream(
    name: str, description: str = None, schema: Union[str, Schema] = None,
) -> OutputDefinition:
    return OutputDefinition(
        port_type=PortType.Stream,
        name=name,
        description=description,
        schema_or_name=schema,
    )


def istream(
    name: str,
    description: str = None,
    schema: Union[str, Schema] = None,
    required: bool = True,
) -> InputDefinition:
    return InputDefinition(
        port_type=PortType.Stream,
        name=name,
        description=description,
        schema_or_name=schema,
        required=required,
    )


def itable(
    name: str,
    description: str = None,
    schema: Union[str, Schema] = None,
    required: bool = True,
) -> InputDefinition:
    return InputDefinition(
        port_type=PortType.Table,
        name=name,
        description=description,
        schema_or_name=schema,
        required=required,
    )


def otable(
    name: str, description: str = None, schema: Union[str, Schema] = None,
) -> OutputDefinition:
    return OutputDefinition(
        port_type=PortType.Table,
        name=name,
        description=description,
        schema_or_name=schema,
    )


def s(name: str):
    return StateDefinition(name=name)


def _ge(s: str, ids_by_path: dict) -> GraphEdge:
    l, r = s.split(" -> ")
    ln, lp = l.split(":")
    rn, rp = r.split(":")
    return GraphEdge(
        input=PortId(node_id=ids_by_path[ln], port=lp),
        output=PortId(node_id=ids_by_path[rn], port=rp),
    )


def _unordered(c: Collection) -> Set:
    s = set(c)
    assert len(c) == len(s)
    return s


def read_manifest(name: str) -> GraphManifest:
    pth = Path(__file__).parent / name / "graph.yml"
    return graph_manifest_from_yaml(pth, allow_errors=True)


def setup_manifest(root: Path, files: Dict[str, str]) -> GraphManifest:
    for path, content in files.items():
        content = textwrap.dedent(content).strip()
        if path.endswith(".py"):
            if not content.startswith("@node"):
                content = f"@node\ndef generated_node(\n{content}\n):\n    pass"
            content = "from basis import *\n\n" + content
        abspath = root / path
        if len(Path(path).parts) > 1:
            abspath.parent.mkdir(parents=True, exist_ok=True)
        abspath.write_text(content)
    return graph_manifest_from_yaml(root / "graph.yml", allow_errors=True)
