from dataclasses import dataclass, asdict
from typing import Any, Union, List, Dict

from commonmodel import Schema

from basis.configuration.path import AbsoluteEdge, DeclaredEdge, PortPath, NodePath
from basis.graph.configured_node import ParameterDefinition, ParameterType, OutputDefinition, PortType, InputDefinition, \
    ConfiguredNode, NodeType, NodeInterface


class _IgnoreType:
    def __repr__(self):
        return 'IGNORE'


IGNORE = _IgnoreType()


@dataclass
class NodeAssertion:
    name: Union[str, _IgnoreType]
    node_type: Union[NodeType, _IgnoreType]
    interface: Union[NodeInterface, _IgnoreType]
    node_depth: Union[int, _IgnoreType]
    description: Union[str, _IgnoreType]
    file_path_to_node_script_relative_to_root: Union[str, _IgnoreType]
    parameter_values: Union[Dict[str, Any], _IgnoreType]
    schedule: Union[str, _IgnoreType]
    declared_edges: Union[List[DeclaredEdge], _IgnoreType]
    absolute_edges: Union[List[AbsoluteEdge], _IgnoreType]


def assert_node(node: ConfiguredNode, assertion: NodeAssertion):
    for k, v in asdict(assertion).items():
        if isinstance(v, _IgnoreType):
            continue
        actual = getattr(node, k)
        assert actual == v, f'{k}: {actual} != {v}'


def assert_nodes(
    nodes: List[ConfiguredNode],
    *expected: NodeAssertion,
    assert_length: bool = True
):
    if assert_length:
        assert len(nodes) == len(expected)

    nodes_by_name = {node.name: node for node in nodes}
    for assertion in expected:
        assert assertion.name in nodes_by_name, f'No node named {assertion.name}'
        assert_node(nodes_by_name[assertion.name], assertion)


# noinspection PyDefaultArgument
def n(
    name: str,
    node_type: Union[NodeType, _IgnoreType] = NodeType.Node,
    interface: Union[List[Union[InputDefinition, OutputDefinition, ParameterDefinition]], _IgnoreType] = IGNORE,
    node_depth: Union[int, _IgnoreType] = 0,
    description: Union[str, None, _IgnoreType] = None,
    file_path: Union[str, _IgnoreType] = IGNORE,
    parameter_values: Union[Dict[str, Any], _IgnoreType] = {},
    schedule: Union[str, None, _IgnoreType] = None,
    declared_edges: Union[List[str], _IgnoreType] = [],
    absolute_edges: Union[List[str], _IgnoreType] = [],
) -> NodeAssertion:
    return NodeAssertion(
        name=name,
        node_type=node_type,
        interface=interface if interface is IGNORE else NodeInterface(
            inputs=[i for i in interface if isinstance(i, InputDefinition)],
            outputs=[i for i in interface if isinstance(i, OutputDefinition)],
            parameters=[i for i in interface if isinstance(i, ParameterDefinition)],
        ),
        node_depth=node_depth,
        description=description,
        file_path_to_node_script_relative_to_root=file_path,
        parameter_values=parameter_values,
        schedule=schedule,
        declared_edges=declared_edges if declared_edges is IGNORE else [de(s) for s in declared_edges],
        absolute_edges=absolute_edges if absolute_edges is IGNORE else [ae(s) for s in absolute_edges],
    )


def p(
    name: str,
    parameter_type: str = None,
    description: str = None,
    default: Any = None,
) -> ParameterDefinition:
    return ParameterDefinition(
        name=name,
        parameter_type=ParameterType(parameter_type) if parameter_type else None,
        description=description,
        default=default,
    )


def ostream(
    name: str,
    description: str = None,
    schema: Union[str, Schema] = None,
) -> OutputDefinition:
    return OutputDefinition(
        port_type=PortType.Stream,
        name=name,
        description=description,
        schema_or_name=schema
    )


def istream(
    name: str,
    description: str = None,
    schema: Union[str, Schema] = None,
    required: bool = True

) -> InputDefinition:
    return InputDefinition(
        port_type=PortType.Stream,
        name=name,
        description=description,
        schema_or_name=schema,
        required=required
    )


def itable(
    name: str,
    description: str = None,
    schema: Union[str, Schema] = None,
    required: bool = True

) -> InputDefinition:
    return InputDefinition(
        port_type=PortType.Table,
        name=name,
        description=description,
        schema_or_name=schema,
        required=required
    )


def otable(
    name: str,
    description: str = None,
    schema: Union[str, Schema] = None,
) -> OutputDefinition:
    return OutputDefinition(
        port_type=PortType.Table,
        name=name,
        description=description,
        schema_or_name=schema
    )


def ae(s: str) -> AbsoluteEdge:
    l, r = s.split(' -> ')
    ln, lp = l.split(':')
    rn, rp = r.split(':')
    return AbsoluteEdge(
        input_path=PortPath(node_path=NodePath(ln), port=lp),
        output_path=PortPath(node_path=NodePath(rn), port=rp),
    )


def de(s: str) -> DeclaredEdge:
    l, r = s.split(' -> ')
    return DeclaredEdge(input_port=l, output_port=r, )
