from __future__ import annotations

import re
from typing import List

from basis.configuration.base import FrozenPydanticBase


class NodePath(str):
    """A dotted path to a node.

    e.g.
    - NodePath('a') # top level node
    - NodePath('a.b.c') # nested node
    """

    @classmethod
    def from_parts(cls, *parts):
        if len(parts) == 1 and isinstance(parts[0], List):
            parts = parts[0]
        return NodePath('.'.join(parts))

    @property
    def parts(self):
        """AbsoluteNodePath('a.b.c').parts == ['a', 'b', 'c']"""
        return self.split('.')

    @property
    def name(self):
        """AbsoluteNodePath('a.b.c').name == 'c'"""
        return self.parts[-1]

    @property
    def parent(self):
        """AbsoluteNodePath('a.b.c').parent == 'a.b'"""
        return '.'.join(self.parents)

    @property
    def parents(self):
        """AbsoluteNodePath('a.b.c').parents == ['a', 'b']"""
        return self.parts[:-1]

    # pydantic methods

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    _regex = re.compile(r'^\w+(?:\.\w+)*$')

    @classmethod
    def __modify_schema__(cls, field_schema):
        field_schema.update(
            pattern=cls._regex.pattern,
            examples=['node', 'graph.node'],
        )

    @classmethod
    def validate(cls, v):
        if not cls._regex.fullmatch(v):
            raise ValueError('invalid alias format')
        return cls(v)

    def __repr__(self):
        return f'NodePath({super().__repr__()})'


class PortPath(FrozenPydanticBase):
    """A NodePath and a port name"""
    node_path: NodePath
    port: str


class AbsoluteEdge(FrozenPydanticBase):
    """An edge with the port names resolved to absolute node paths"""
    input_path: PortPath
    output_path: PortPath


class DeclaredEdge(FrozenPydanticBase):
    """An edge like `my_table -> input_port` explicitly declared in the graph yml"""
    input_port: str
    output_port: str
