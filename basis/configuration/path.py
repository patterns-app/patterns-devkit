from __future__ import annotations

from dataclasses import dataclass
import re
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from basis.configuration.base import FrozenPydanticBase

re_port_path = re.compile(r"\s*(\w+)\[(\w+)\]\s*")
re_node_connection = re.compile(r"^({0})=>({0})$".format(re_port_path.pattern))


def join_node_paths(*paths: str) -> str:
    p = ".".join(paths)
    p = p.replace("..", ".")
    p = p.strip(".")
    return p


class PortType(str, Enum):
    TABLE = "table"
    STREAM = "stream"
    PARAMETER = "parameter"


class PortPath(FrozenPydanticBase):
    node: str
    port: str

    @classmethod
    def from_str(cls, path: str) -> PortPath:
        m = re.match(r"^{0}$".format(re_port_path.pattern), path)
        if m is None:
            raise ValueError(path)
        return cls(node=m.group(1), port=m.group(2))

    def __str__(self) -> str:
        return f"{self.node}[{self.port}]"


class NodeConnection(FrozenPydanticBase):
    input_path: PortPath
    output_path: PortPath

    @classmethod
    def from_str(cls, connection: str) -> NodeConnection:
        m = re_node_connection.match(connection)
        if m is None:
            raise ValueError(connection)
        return cls(
            input_path=PortPath.from_str(m.group(1)),
            output_path=PortPath.from_str(m.group(4)),
        )

    def __str__(self) -> str:
        return f"{self.input_path} => {self.output_path}"


class AbsoluteNodePath(FrozenPydanticBase):
    node: str
    path_to_node: Optional[str] = None

    @classmethod
    def from_str(cls, path: str) -> AbsoluteNodePath:
        parts = path.split(".")
        return AbsoluteNodePath(node=parts[-1], path_to_node=".".join(parts[:-1]))

    def __str__(self) -> str:
        return f"{self.path_to_node}.{self.node}"


class AbsolutePortPath(FrozenPydanticBase):
    absolute_node_path: AbsoluteNodePath
    port: str

    @classmethod
    def from_str(cls, path: str) -> AbsoluteNodePath:
        parts = path.split(".")
        port_path = PortPath.from_str(parts[-1])
        return AbsolutePortPath(
            port=port_path.port,
            absolute_node_path=AbsoluteNodePath.from_str(
                join_node_paths(".".join(parts[:-1]), port_path.node)
            ),
        )

    def __str__(self) -> str:
        return f"{self.absolute_node_path}[{self.port}]"


class AbsoluteNodeConnection(FrozenPydanticBase):
    input_path: AbsolutePortPath
    output_path: AbsolutePortPath

    def __str__(self) -> str:
        return f"{self.input_path} => {self.output_path}"


def as_absolute_port_path(
    path: PortPath, parent_absolute_path: str
) -> AbsolutePortPath:
    parent_abs_path = AbsoluteNodePath.from_str(parent_absolute_path)
    if path.node == "self" or path.node == "" or path.node == parent_abs_path.node:
        # It's a ref to the parent
        return AbsolutePortPath(absolute_node_path=parent_abs_path, port=path.port)
    # Otherwise it's a ref to sibling
    return AbsolutePortPath.from_str(join_node_paths(parent_absolute_path, str(path)))


def as_absolute_connection(
    connection: NodeConnection, parent_absolute_path: str
) -> AbsoluteNodeConnection:
    return AbsoluteNodeConnection(
        input_path=as_absolute_port_path(connection.input_path, parent_absolute_path),
        output_path=as_absolute_port_path(connection.output_path, parent_absolute_path),
    )
