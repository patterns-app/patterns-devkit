from basis.configuration.path import (
    AbsoluteNodePath,
    AbsolutePortPath,
    NodeConnection,
    PortPath,
    as_absolute_port_path,
)


def test_paths():
    p = PortPath.from_str("path[port]")
    assert p.node == "path"
    assert p.port == "port"
    c = NodeConnection.from_str("path[port] => node[other]")
    assert c.input_path.node == "path"
    assert c.input_path.port == "port"
    assert c.output_path.node == "node"
    assert c.output_path.port == "other"
    p = AbsoluteNodePath.from_str("my.node.path")
    assert p.node == "path"
    assert p.path_to_node == "my.node"
    p = AbsolutePortPath.from_str("my.node.path[port]")
    assert str(p.absolute_node_path) == "my.node.path"
    assert p.port == "port"
    p = as_absolute_port_path(PortPath.from_str("path[port]"), "my.abs")
    assert str(p.absolute_node_path) == "my.abs.path"
    assert p.port == "port"
    p = as_absolute_port_path(PortPath.from_str("self[port]"), "my.abs")
    assert str(p.absolute_node_path) == "my.abs"
    assert p.port == "port"
    # TODO: do we want this?
    # p = as_absolute_port_path(PortPath.from_str("port]"), "my.abs")
    # assert str(p.absolute_node_path) == "my.abs"
    # assert p.port == "port"
