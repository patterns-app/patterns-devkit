from typing import Any, Union

from commonmodel import Schema

from basis.configuration.path import AbsoluteEdge, PortPath, NodePath, DeclaredEdge
from basis.graph.configured_node import ParameterDefinition, ParameterType, OutputDefinition, PortType, InputDefinition


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
