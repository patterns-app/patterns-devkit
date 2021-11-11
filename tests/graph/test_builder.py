from pathlib import Path
from typing import Union, Any

from commonmodel import Schema

from basis import GraphManifest
from basis.configuration.path import AbsoluteEdge, PortPath, NodePath, DeclaredEdge
from basis.graph.builder import graph_manifest_from_yaml
from basis.graph.configured_node import CURRENT_MANIFEST_SCHEMA_VERSION, ConfiguredNode, NodeInterface, \
    OutputDefinition, PortType, InputDefinition, ParameterType, ParameterDefinition, NodeType


def _build_manifest(name: str) -> GraphManifest:
    p = Path(__file__).parent / name / 'graph.yml'
    return graph_manifest_from_yaml(p)


def test_flat_graph():
    manifest = _build_manifest('flat_graph')
    assert manifest.graph_name == 'Test graph'
    assert manifest.manifest_version == CURRENT_MANIFEST_SCHEMA_VERSION
    assert len(manifest.nodes) == 5

    assert manifest.nodes[0] == ConfiguredNode(
        name='source',
        node_type=NodeType.Node,
        absolute_node_path=NodePath('source'),
        interface=NodeInterface(
            inputs=[],
            outputs=[ostream('source_stream')],
            parameters=[]
        ),
        node_depth=0,
        file_path_to_node_script_relative_to_root='source.py',
        parameter_values={},
        declared_edges=[],
        absolute_edges=[ae('source:source_stream -> pass:source_stream')]
    )

    assert manifest.nodes[1] == ConfiguredNode(
        name='pass',
        node_type=NodeType.Node,
        absolute_node_path=NodePath('pass'),
        description='Passthrough Desc',
        interface=NodeInterface(
            inputs=[istream('source_stream', 'in desc', 'TestSchema'), istream('optional_stream', required=False)],
            outputs=[ostream('passthrough_stream', 'out desc', 'TestSchema2')],
            parameters=[p('explicit_param', 'bool', 'param desc', False), p('plain_param')]
        ),
        node_depth=0,
        file_path_to_node_script_relative_to_root='passthrough.py',
        parameter_values={'plain_param': 'test value'},
        declared_edges=[],
        absolute_edges=[ae('source:source_stream -> pass:source_stream'),
                        ae('pass:passthrough_stream -> mapper:input_stream')]
    )

    assert manifest.nodes[2] == ConfiguredNode(
        name='mapper',
        node_type=NodeType.Node,
        absolute_node_path=NodePath('mapper'),
        interface=NodeInterface(
            inputs=[istream('input_stream')],
            outputs=[otable('output_table')],
            parameters=[]
        ),
        node_depth=0,
        file_path_to_node_script_relative_to_root='mapper/mapper.py',
        parameter_values={},
        declared_edges=[de('passthrough_stream -> input_stream'), de('output_table -> query_table')],
        absolute_edges=[ae('pass:passthrough_stream -> mapper:input_stream'),
                        ae('mapper:output_table -> query:query_table')]
    )

    assert manifest.nodes[3] == ConfiguredNode(
        name='query',
        node_type=NodeType.Node,
        absolute_node_path=NodePath('query'),
        interface=NodeInterface(
            inputs=[itable('query_table')],
            outputs=[otable('sink_table')],
            parameters=[p('num', parameter_type='int')]
        ),
        node_depth=0,
        file_path_to_node_script_relative_to_root='query.sql',
        parameter_values={'num': 0},
        declared_edges=[],
        absolute_edges=[ae('mapper:output_table -> query:query_table'),
                        ae('query:sink_table -> sink:sink_table')]
    )

    assert manifest.nodes[4] == ConfiguredNode(
        name='sink',
        node_type=NodeType.Node,
        absolute_node_path=NodePath('sink'),
        interface=NodeInterface(
            inputs=[itable('sink_table')],
            outputs=[],
            parameters=[]
        ),
        node_depth=0,
        file_path_to_node_script_relative_to_root='sink.py',
        parameter_values={},
        declared_edges=[],
        absolute_edges=[ae('query:sink_table -> sink:sink_table')]
    )

# TODO: disabled pending dynamic import fix
# def test_fanout_graph():
#     manifest = _build_manifest('fanout_graph')
#     assert manifest.graph_name == 'fanout_graph'
#     assert len(manifest.nodes) == 4
#     assert manifest.nodes[0] == ConfiguredNode(
#         name='source',
#         node_type=NodeType.Node,
#         absolute_node_path=NodePath('source'),
#         interface=NodeInterface(
#             inputs=[],
#             outputs=[ostream('source_stream')],
#             parameters=[]
#         ),
#         node_depth=0,
#         file_path_to_node_script_relative_to_root='source.py',
#         parameter_values={},
#         declared_edges=[],
#         absolute_edges=[ae('source:source_stream -> pass1:pass_in'),
#                         ae('source:source_stream -> pass2:pass_in')]
#     )
#     assert manifest.nodes[1] == ConfiguredNode(
#         name='pass1',
#         node_type=NodeType.Node,
#         absolute_node_path=NodePath('pass1'),
#         interface=NodeInterface(
#             inputs=[istream('pass_in')],
#             outputs=[ostream('pass_out')],
#             parameters=[]
#         ),
#         node_depth=0,
#         file_path_to_node_script_relative_to_root='pass.py',
#         parameter_values={},
#         declared_edges=[de('source_stream -> pass_in'), de('pass_out -> sink_stream1')],
#         absolute_edges=[ae('source:source_stream -> pass1:pass_in'),
#                         ae('pass1:pass_out -> sink:sink_stream1')]
#     )
#     assert manifest.nodes[2] == ConfiguredNode(
#         name='pass2',
#         node_type=NodeType.Node,
#         absolute_node_path=NodePath('pass2'),
#         interface=NodeInterface(
#             inputs=[istream('pass_in')],
#             outputs=[ostream('pass_out')],
#             parameters=[]
#         ),
#         node_depth=0,
#         file_path_to_node_script_relative_to_root='pass.py',
#         parameter_values={},
#         declared_edges=[de('source_stream -> pass_in'), de('pass_out -> sink_stream2')],
#         absolute_edges=[ae('source:source_stream -> pass2:pass_in'),
#                         ae('pass2:pass_out -> sink:sink_stream2')]
#     )
#     assert manifest.nodes[3] == ConfiguredNode(
#         name='sink',
#         node_type=NodeType.Node,
#         absolute_node_path=NodePath('sink'),
#         interface=NodeInterface(
#             inputs=[istream('sink_stream1'), istream('sink_stream2')],
#             outputs=[],
#             parameters=[]
#         ),
#         node_depth=0,
#         file_path_to_node_script_relative_to_root='sink.py',
#         parameter_values={},
#         declared_edges=[],
#         absolute_edges=[ae('pass1:pass_out -> sink:sink_stream1'),
#                         ae('pass2:pass_out -> sink:sink_stream2')]
#     )


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
