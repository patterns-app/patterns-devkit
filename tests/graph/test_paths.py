from basis.configuration.path import (
    NodePath,
)


def test_top_level_node_path():
    p = NodePath('a')

    assert NodePath.from_parts('a') == p
    assert NodePath.from_parts(['a']) == p
    assert p.parts == ['a']
    assert p.name == 'a'
    assert p.parent == ''
    assert p.parents == []
    assert str(p) == 'a'


def test_nested_node_path():
    p = NodePath('a.b.c')

    assert NodePath.from_parts('a', 'b', 'c') == p
    assert NodePath.from_parts(['a', 'b', 'c']) == p
    assert p.parts == ['a', 'b', 'c']
    assert p.name == 'c'
    assert p.parent == 'a.b'
    assert p.parents == ['a', 'b']
    assert str(p) == 'a.b.c'
