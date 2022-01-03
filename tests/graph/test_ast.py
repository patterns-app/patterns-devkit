from pathlib import Path

from basis.utils.ast_parser import read_interface_from_py_node_file
from tests.graph.utils import istream, ostream, p


def test_interface_parse():
    i = read_interface_from_py_node_file(
        Path(__file__).parent / "flat_graph" / "passthrough.py"
    )

    assert len(i.inputs) == 2
    assert i.inputs[0] == istream("source_stream", "in desc", "TestSchema")
    assert i.inputs[1] == istream("optional_stream", required=False)

    assert len(i.outputs) == 1
    assert i.outputs[0] == ostream("passthrough_stream", "out desc", "TestSchema2")

    assert len(i.parameters) == 2
    assert i.parameters[0] == p("explicit_param", "bool", "param desc", False)
    assert i.parameters[1] == p("plain_param")

    assert i.state is not None
    assert i.state.name == "state_param"
    
    assert len(i.connections) == 1
    assert i.connections[0] == "state_param"
