from __future__ import annotations

import random
from base64 import b32encode
from hashlib import blake2b
from typing import Optional

from basis.configuration.base import FrozenPydanticBase


class NodeId(str):
    """An id that is unique within a graph.

    Represented as an eight character base32 encoded string.
    """

    @classmethod
    def random(cls) -> "NodeId":
        """Generate a new random id"""
        # random.randbytes was added in 3.9
        b = bytes([random.randint(0, 255) for _ in range(5)])
        return cls.from_bytes(b)

    @classmethod
    def from_bytes(cls, b: bytes) -> "NodeId":
        """Encode a byte string as a base32 NodeId"""
        return cls(b32encode(b).decode().lower())

    @classmethod
    def from_name(cls, node_name: str, parent_id: Optional[NodeId]) -> "NodeId":
        """Generate a new id deterministically from a node name and the id of its parent.

        >> NodeId.from_name('node name', 'parentid')
        """
        # We choose BLAKE2 as the hash function due to its speed and configurable digest size
        # We choose 40 bits of state since that can be base32 encoded with no padding, and
        # in a graph with 50 nodes, the odds of collision is less than 10⁻⁹.
        h = blake2b(digest_size=5)
        if parent_id:
            h.update(parent_id.encode())
        h.update(node_name.encode())
        return cls.from_bytes(h.digest())


class PortId(FrozenPydanticBase):
    """A NodeId and a port name"""

    node_id: NodeId
    port: str


class GraphEdge(FrozenPydanticBase):
    input: PortId
    output: PortId
