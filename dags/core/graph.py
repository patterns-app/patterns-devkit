from __future__ import annotations

from copy import copy
from pprint import pprint
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import networkx as nx
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, event, or_
from sqlalchemy.orm import RelationshipProperty, Session, relationship
from sqlalchemy.sql.sqltypes import JSON, DateTime, Enum

from dags.core.metadata.orm import BaseModel
from dags.core.node import Node, NodeLike, create_node, inputs_as_nodes
from dags.core.pipe import PipeLike
from dags.utils.common import md5_hash, remove_dupes
from loguru import logger

if TYPE_CHECKING:
    from dags import Environment


class NodeDoesNotExist(KeyError):
    pass


class GraphMetadata(BaseModel):
    hash = Column(String, primary_key=True)
    adjacency = Column(JSON)

    def __repr__(self):
        return self._repr(
            id=self.id,
            hash=self.hash,
        )


def hash_adjacency(adjacency: List[Tuple[str, Dict]]) -> str:
    return md5_hash(str(adjacency))


class Graph:
    def __init__(self, env: Environment, nodes: Iterable[Node] = None):
        self.env = env
        self._nodes: Dict[str, Node] = {}
        if nodes:
            for n in nodes:
                self._add_node(n)

    def __str__(self):
        s = "Nodes:\n------\n" + "\n".join(self._nodes.keys())
        return s

    def get_metadata_obj(self) -> GraphMetadata:
        adjacency = self.adjacency_list()
        return GraphMetadata(hash=hash_adjacency(adjacency), adjacency=adjacency)

    def add_node(self, key: str, pipe: Union[PipeLike, str], **kwargs: Any) -> Node:
        from dags.core.node import Node

        if isinstance(pipe, str):
            pipe = self.env.get_pipe(pipe)
        node = create_node(self, key, pipe, **kwargs)
        self._add_node(node)
        return node

    def _add_node(self, node: Node):
        if node.key in self._nodes:
            raise KeyError(f"Duplicate node key {node.key}")
        self._nodes[node.key] = node

    def remove_node(self, node: Node):
        del self._nodes[node.key]

    def get_node(self, key: NodeLike) -> Node:
        if isinstance(key, Node):
            return key
        assert isinstance(key, str)
        return self._nodes[key]

    def has_node(self, key: str) -> bool:
        return key in self._nodes

    def get_node_like(self, node_like: NodeLike) -> Node:
        if isinstance(node_like, Node):
            return node_like
        return self.get_node(node_like)

    def all_nodes(self) -> Iterable[Node]:
        return self._nodes.values()

    # def copy(self, **kwargs):
    #     return Graph(
    #         env=self.env, nodes=self._nodes.values()
    #     )

    def get_or_create_dataset_nodes(self, node: Node) -> List[Node]:
        keys = node.get_dataset_node_keys()
        ds_nodes = []
        for k in keys:
            try:
                ds_nodes.append(self.get_node(k))
            except KeyError:
                break
        else:
            return ds_nodes

        ds_nodes = node.create_dataset_nodes()
        for dsn in ds_nodes:
            self._add_node(dsn)
        return ds_nodes

    def validate_graph(self) -> bool:
        # TODO
        #   validate node keys are valid
        #   validate pipes are valid
        #   validate types are valid
        #   etc
        pass

    def as_nx_graph(self) -> nx.DiGraph:
        g = nx.DiGraph()
        for node in self.all_nodes():
            g.add_node(node.key)
            inputs = node.get_declared_input_nodes()
            for input_node in inputs.values():
                if input_node.node.key not in self._nodes:
                    # Don't include nodes not in graph (could be a sub-graph)
                    continue
                g.add_node(input_node.node.key)
                g.add_edge(input_node.node.key, node.key)
            # TODO: self ref edge?
        return g

    def adjacency_list(self):
        return list(self.as_nx_graph().adjacency())

    def get_all_upstream_dependencies_in_execution_order(
        self, node: Node
    ) -> List[Node]:
        g = self.as_nx_graph()
        node_keys = self._get_all_upstream_dependencies_in_execution_order(g, node.key)
        return [self.get_node(name) for name in node_keys]

    def _get_all_upstream_dependencies_in_execution_order(
        self, g: nx.DiGraph, node: str
    ) -> List[str]:
        nodes = []
        for parent_node in g.predecessors(node):
            if parent_node == node:
                # Ignore self-ref cycles
                continue
            parent_deps = self._get_all_upstream_dependencies_in_execution_order(
                g, parent_node
            )
            nodes.extend(parent_deps)
        nodes.append(node)
        # May have added nodes twice, just keep first reference:
        return remove_dupes(nodes)

    def get_all_nodes_in_execution_order(self) -> List[Node]:
        g = self.as_nx_graph()
        return [self.get_node(name) for name in nx.topological_sort(g)]
