import pytest

from basis import node, InputStream
from basis.node.node import NodeFunction


def test_node_decorator():
    def bad_type(p=1):
        pass

    def no_args():
        pass

    def kwargs(i=InputStream, **kwargs):
        pass

    with pytest.raises(TypeError):
        node(bad_type)
    with pytest.raises(TypeError):
        node(no_args)
    assert isinstance(node(kwargs), NodeFunction)  # **kwargs ok
