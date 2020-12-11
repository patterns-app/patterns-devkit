from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import asdict
from importlib import import_module
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple, Union

from loguru import logger
from snapflow.core.component import ComponentLibrary
from snapflow.core.metadata.orm import BaseModel
from snapflow.core.module import DEFAULT_LOCAL_MODULE, SnapflowModule
from snapflow.core.typing.schema import GeneratedSchema, Schema, SchemaLike
from sqlalchemy.orm import Session, close_all_sessions, sessionmaker

if TYPE_CHECKING:
    from snapflow.core.storage.storage import (
        Storage,
        new_local_memory_storage,
        StorageClass,
    )
    from snapflow.core.pipe import Pipe
    from snapflow.core.node import Node, NodeLike
    from snapflow.core.runnable import ExecutionContext
    from snapflow.core.data_block import DataBlock
    from snapflow.core.graph import Graph, DeclaredGraph, DEFAULT_GRAPH

DEFAULT_METADATA_STORAGE_URL = "sqlite://"  # in-memory sqlite


class Environment:
    library: ComponentLibrary
    storages: List[Storage]
    metadata_storage: Storage
    session: Session

    def __init__(
        self,
        name: str = None,
        metadata_storage: Union["Storage", str] = None,
        add_default_python_runtime: bool = True,
        initial_modules: List[SnapflowModule] = None,  # Defaults to `core` module
    ):
        from snapflow.core.runtime import Runtime
        from snapflow.core.runtime import RuntimeClass
        from snapflow.core.runtime import RuntimeEngine
        from snapflow.core.storage.storage import Storage, new_local_memory_storage
        from snapflow.modules import core

        self.name = name
        if metadata_storage is None:
            metadata_storage = DEFAULT_METADATA_STORAGE_URL
            logger.warning(
                f"No metadata storage specified, using default sqlite db `{DEFAULT_METADATA_STORAGE_URL}`"
            )
        if isinstance(metadata_storage, str):
            metadata_storage = Storage.from_url(metadata_storage)
        if metadata_storage is None:
            raise Exception("Must specify metadata_storage or allow default")
        self.metadata_storage = metadata_storage
        self.initialize_metadata_database()
        self._local_module = DEFAULT_LOCAL_MODULE
        self.library = ComponentLibrary()
        self.storages = []
        self.runtimes = []
        self._metadata_sessions: List[Session] = []
        if add_default_python_runtime:
            self.runtimes.append(
                Runtime(
                    url="python://local",
                    runtime_class=RuntimeClass.PYTHON,
                    runtime_engine=RuntimeEngine.LOCAL,
                )
            )
        if initial_modules is None:
            initial_modules = [core]
        for m in initial_modules:
            self.add_module(m)

        self._local_memory_storage = new_local_memory_storage()
        self.add_storage(self._local_memory_storage)
        self.session = self.get_new_metadata_session()

    def initialize_metadata_database(self):
        from snapflow.core.metadata.listeners import add_persisting_sdb_listener
        from snapflow.core.storage.storage import StorageClass

        if self.metadata_storage.storage_class != StorageClass.DATABASE:
            raise ValueError(
                f"metadata storage expected a database, got {self.metadata_storage}"
            )
        conn = self.metadata_storage.get_database_api(self).get_engine()
        BaseModel.metadata.create_all(conn)
        self.Session = sessionmaker(bind=conn)
        add_persisting_sdb_listener(self.Session)

    def get_new_metadata_session(self) -> Session:
        sess = self.Session()
        self._metadata_sessions.append(sess)
        return sess

    def clean_up_db_sessions(self):
        from snapflow.db.api import dispose_all

        close_all_sessions()
        dispose_all()

    def close_sessions(self):
        for s in self._metadata_sessions:
            s.close()
        self._metadata_sessions = []

    def get_default_local_memory_storage(self) -> Storage:
        return self._local_memory_storage

    def get_local_module(self) -> SnapflowModule:
        return self._local_module

    def get_module_order(self) -> List[str]:
        return self.library.module_lookup_names

    def get_schema(self, schema_like: SchemaLike) -> Schema:
        if isinstance(schema_like, Schema):
            return schema_like
        try:
            return self.library.get_schema(schema_like)
        except KeyError:
            schema = self.get_generated_schema(schema_like)
            if schema is None:
                raise KeyError(schema_like)
            return schema

    def add_schema(self, schema: Schema):
        self.library.add_schema(schema)

    def get_generated_schema(self, schema_like: SchemaLike) -> Optional[Schema]:
        if isinstance(schema_like, str):
            key = schema_like
        elif isinstance(schema_like, Schema):
            key = schema_like.key
        else:
            raise TypeError(schema_like)
        got = self.session.query(GeneratedSchema).get(key)
        if got is None:
            return None
        return got.as_schema()

    def add_new_generated_schema(self, schema: Schema):
        logger.debug(f"Adding new generated schema {schema}")
        if schema.key in self.library.schemas:
            # Already exists
            return
        got = GeneratedSchema(key=schema.key, definition=asdict(schema))
        self.session.add(got)
        self.session.flush([got])
        self.library.add_schema(schema)

    def all_schemas(self) -> List[Schema]:
        return self.library.all_schemas()

    def get_pipe(self, pipe_like: str) -> Pipe:
        return self.library.get_pipe(pipe_like)

    def add_pipe(self, pipe: Pipe):
        self.library.add_pipe(pipe)

    def all_pipes(self) -> List[Pipe]:
        return self.library.all_pipes()

    def add_module(self, *modules: SnapflowModule):
        for module in modules:
            self.library.add_module(module)

    @contextmanager
    def session_scope(self, **kwargs):
        session = self.Session(**kwargs)
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_default_storage(self) -> Storage:
        from snapflow.core.storage.storage import StorageClass

        if len(self.storages) == 1:
            return self.storages[0]
        for s in self.storages:
            if s.url == self.metadata_storage.url:
                continue
            if s.storage_class == StorageClass.MEMORY:
                continue
            return s
        return self.storages[0]

    def get_execution_context(
        self, graph: Graph, target_storage: Storage = None, **kwargs
    ) -> ExecutionContext:
        from snapflow.core.storage.storage import StorageClass
        from snapflow.core.runnable import ExecutionContext

        if target_storage is None:
            target_storage = self.get_default_storage()
        if target_storage.storage_class == StorageClass.MEMORY:
            logging.warning(
                "Using MEMORY storage -- results of execution will NOT "
                "be persisted. Add a database or file storage to persist results."
            )
        args = dict(
            graph=graph,
            env=self,
            metadata_session=self.session,
            runtimes=self.runtimes,
            storages=self.storages,
            target_storage=target_storage,
            local_memory_storage=self.get_default_local_memory_storage(),
        )
        args.update(**kwargs)
        return ExecutionContext(**args)  # type: ignore

    @contextmanager
    def execution(self, graph: Graph, target_storage: Storage = None, **kwargs):
        from snapflow.core.runnable import ExecutionManager

        self.session.begin_nested()
        ec = self.get_execution_context(graph, target_storage=target_storage, **kwargs)
        em = ExecutionManager(ec)
        logger.debug(f"executing on graph {graph.adjacency_list()}")
        try:
            yield em
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            # TODO:
            # self.validate_and_clean_data_blocks(delete_intermediate=True)
            self.session.close()

    def _get_graph_and_node(
        self,
        node_like: Optional[NodeLike] = None,
        graph: Optional[Union[Graph, DeclaredGraph]] = None,
    ) -> Tuple[Optional[Node], Graph]:
        from snapflow.core.graph import DEFAULT_GRAPH, DeclaredGraph, Graph
        from snapflow.core.node import Node, DeclaredNode

        node = None
        if graph is None:
            if hasattr(node_like, "graph"):
                graph = node_like.graph
            else:
                graph = DEFAULT_GRAPH
        if isinstance(graph, DeclaredGraph):
            graph = graph.instantiate(self)
        if node_like is not None:
            node = node_like
            if isinstance(node, str):
                node = graph.get_node(node_like)
            if isinstance(node, DeclaredNode):
                node = node.instantiate(self, graph)
            assert isinstance(node, Node)
        assert isinstance(graph, Graph)
        return node, graph

    def produce(
        self,
        node_like: Optional[NodeLike] = None,
        graph: Union[Graph, DeclaredGraph] = None,
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ) -> Optional[DataBlock]:
        node, graph = self._get_graph_and_node(node_like, graph)
        if node is not None:
            dependencies = graph.get_all_upstream_dependencies_in_execution_order(node)
        else:
            dependencies = graph.get_all_nodes_in_execution_order()
        output = None
        for dep in dependencies:
            with self.execution(graph, **execution_kwargs) as em:
                output = em.run(dep, to_exhaustion=to_exhaustion)
        return output

    def run_node(
        self,
        node_like: Optional[NodeLike] = None,
        graph: Union[Graph, DeclaredGraph] = None,
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ) -> Optional[DataBlock]:
        node, graph = self._get_graph_and_node(node_like, graph)

        logger.debug(f"Running: {node_like}")
        with self.execution(graph, **execution_kwargs) as em:
            return em.run(node_like, to_exhaustion=to_exhaustion)

    def run_graph(
        self,
        graph: Union[Graph, DeclaredGraph],
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ):
        from snapflow.core.graph import DeclaredGraph

        if isinstance(graph, DeclaredGraph):
            graph = graph.instantiate(self)
        nodes = graph.get_all_nodes_in_execution_order()
        for node in nodes:
            with self.execution(graph, **execution_kwargs) as em:
                em.run(node, to_exhaustion=to_exhaustion)

    # def get_latest_output(self, node: Node) -> Optional[DataBlock]:
    #     session = self.get_new_metadata_session()  # TODO: hanging session
    #     ctx = self.get_execution_context(session)
    #     return node.get_latest_output(ctx)

    def add_storage(
        self, storage_like: Union[Storage, str], add_runtime: bool = True
    ) -> Storage:
        from snapflow.core.storage.storage import Storage

        if isinstance(storage_like, str):
            sr = Storage.from_url(storage_like)
        elif isinstance(storage_like, Storage):
            sr = storage_like
        else:
            raise TypeError
        for s in self.storages:
            if s.url == sr.url:
                return s
        self.storages.append(sr)
        if add_runtime:
            from snapflow.core.runtime import Runtime

            try:
                rt = Runtime.from_storage(sr)
                self.runtimes.append(rt)
            except ValueError:
                pass
        return sr

    def validate_and_clean_data_blocks(
        self, delete_memory=True, delete_intermediate=False, force: bool = False
    ):
        from snapflow.core.data_block import (
            DataBlockMetadata,
            StoredDataBlockMetadata,
        )

        if delete_memory:
            deleted = (
                self.session.query(StoredDataBlockMetadata)
                .filter(StoredDataBlockMetadata.storage_url.startswith("memory:"))
                .delete(False)
            )
            print(f"{deleted} Memory StoredDataBlocks deleted")

        for block in self.session.query(DataBlockMetadata).filter(
            ~DataBlockMetadata.stored_data_blocks.any()
        ):
            print(f"#{block.id} {block.nominal_schema_key} is orphaned! SAD")
        if delete_intermediate:
            # TODO: does no checking if they are unprocessed or not...
            if not force:
                d = input(
                    "Are you sure you want to delete ALL intermediate DataBlocks? There is no undoing this operation. y/N?"
                )
                if not d or d.lower()[0] != "y":
                    return
            # Delete blocks with no DataSet
            cnt = (
                self.session.query(DataBlockMetadata)
                .filter(
                    ~DataBlockMetadata.data_sets.any(),
                )
                .update({DataBlockMetadata.deleted: True}, synchronize_session=False)
            )
            print(f"{cnt} intermediate DataBlocks deleted")


# Shortcuts
def produce(
    *args,
    env: Optional[Environment] = None,
    modules: Optional[List[SnapflowModule]] = None,
    **kwargs: Any,
) -> Optional[DataBlock]:
    if env is None:
        env = Environment()
    if modules is not None:
        for module in modules:
            env.add_module(module)
    return env.produce(*args, **kwargs)


def run_node(
    *args,
    env: Optional[Environment] = None,
    modules: Optional[List[SnapflowModule]] = None,
    **kwargs: Any,
) -> Optional[DataBlock]:
    if env is None:
        env = Environment()
    if modules is not None:
        for module in modules:
            env.add_module(module)
    return env.run_node(*args, **kwargs)


def run_graph(
    *args,
    env: Optional[Environment] = None,
    modules: Optional[List[SnapflowModule]] = None,
    **kwargs: Any,
):
    if env is None:
        env = Environment()
    if modules is not None:
        for module in modules:
            env.add_module(module)
    return env.run_graph(*args, **kwargs)


# Not supporting yml project config atm
# def load_environment_from_yaml(yml) -> Environment:
#     from snapflow.core.storage.storage import StorageResource
#
#     env = Environment(
#         metadata_storage=yml.get("metadata_storage", None),
#         add_default_python_runtime=yml.get("add_default_python_runtime", True),
#     )
#     for url in yml.get("storages"):
#         env.add_storage(StorageResource.from_url(url))
#     for module_name in yml.get("module_lookup_names"):
#         m = import_module(module_name)
#         env.add_module(m)
#     return env


def load_environment_from_project(project: Any) -> Environment:
    from snapflow.core.storage.storage import Storage

    env = Environment(
        metadata_storage=getattr(project, "metadata_storage", None),
        add_default_python_runtime=getattr(project, "add_default_python_runtime", True),
    )
    for url in getattr(project, "storages", []):
        env.add_storage(Storage.from_url(url))
    for module_name in getattr(project, "module_lookup_names", []):
        m = import_module(module_name)
        env.add_module(m)  # type: ignore  # We hijack the module
    return env


def current_env(cfg_module: str = "project") -> Environment:
    import sys

    sys.path.append(os.getcwd())
    cfg = import_module(cfg_module)
    return load_environment_from_project(cfg)
