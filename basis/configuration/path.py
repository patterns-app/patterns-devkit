from __future__ import annotations

import random
from base64 import b32encode
from hashlib import blake2b

from basis.configuration.base import FrozenPydanticBase


class NodeId(str):
    """An id that is unique within a graph.

    Represented as an eight character base32 encoded string.
    """

    @classmethod
    def random(cls) -> 'NodeId':
        """Generate a new random id"""
        return cls.from_bytes(b32encode(random.randbytes(5)))

    @classmethod
    def from_bytes(cls, b: bytes) -> 'NodeId':
        """Encode a byte string as a base32 NodeId"""
        return cls(b32encode(b).decode().lower())

    @classmethod
    def from_path(cls, *parts: str) -> 'NodeId':
        """Generate a new id deterministically from a node name and the ids of its parents.

        >> NodeId.from_path('grandparent', 'parent', 'node')
        """
        # We choose BLAKE2 as the hash function due to its speed and configurable digest size
        # We choose 40 bits of state since that can be base32 encoded with no padding, and
        # in a graph with 50 nodes, the odds of collision is less than 10⁻⁹.
        h = blake2b(digest_size=5)
        for part in parts:
            h.update(part.encode())
        return cls.from_bytes(h.digest())


class PortId(FrozenPydanticBase):
    """A NodeId and a port name"""
    node_id: NodeId
    port: str


class AbsoluteEdge(FrozenPydanticBase):
    """An edge with the port names resolved to absolute node paths"""
    input_path: PortId
    output_path: PortId


class DeclaredEdge(FrozenPydanticBase):
    """An edge like `my_table -> input_port` explicitly declared in the graph yml"""
    input_port: str
    output_port: str
