from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import asdict
from importlib import import_module
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from sqlalchemy.orm import Session, close_all_sessions, sessionmaker

from dags.core.component import ComponentLibrary
from dags.core.metadata.orm import BaseModel
from dags.core.module import DEFAULT_LOCAL_MODULE, DagsModule
from dags.core.typing.object_schema import (
    GeneratedObjectSchema,
    ObjectSchema,
    ObjectSchemaLike,
)
from dags.logging.event import Event, EventHandler, EventSubject, event_factory
from loguru import logger

if TYPE_CHECKING:
    from dags.core.storage.storage import (
        Storage,
        new_local_memory_storage,
        StorageClass,
    )
    from dags.core.pipe import Pipe
    from dags.core.node import Node
    from dags.core.runnable import ExecutionContext
    from dags.core.data_block import DataBlock
    from dags.core.graph import Graph


DEFAULT_METADATA_STORAGE_URL = "sqlite:///.dags_metadata.db"


class Environment:
    library: ComponentLibrary
    storages: List[Storage]
    metadata_storage: Storage
    event_handlers: List[EventHandler]

    def __init__(
        self,
        name: str = None,
        metadata_storage: Union["Storage", str] = None,
        add_default_python_runtime: bool = True,
        initial_modules: List[DagsModule] = None,  # Defaults to `core` module
        event_handlers: List[EventHandler] = None,
    ):
        from dags.core.runtime import Runtime
        from dags.core.runtime import RuntimeClass
        from dags.core.runtime import RuntimeEngine
        from dags.core.storage.storage import Storage, new_local_memory_storage
        from dags.modules import core

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

        self.event_handlers = event_handlers or []
        self._local_memory_storage = new_local_memory_storage()
        self.add_storage(self._local_memory_storage)

    def initialize_metadata_database(self):
        from dags.core.metadata.listeners import add_persisting_sdb_listener
        from dags.core.storage.storage import StorageClass

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
        from dags.db.api import dispose_all

        close_all_sessions()
        dispose_all()

    def close_sessions(self):
        for s in self._metadata_sessions:
            s.close()
        self._metadata_sessions = []

    def get_default_local_memory_storage(self) -> Storage:
        return self._local_memory_storage

    def get_local_module(self) -> DagsModule:
        return self._local_module

    def get_module_order(self) -> List[str]:
        return self.library.module_lookup_names

    def get_schema(self, schema_like: ObjectSchemaLike) -> ObjectSchema:
        if isinstance(schema_like, ObjectSchema):
            return schema_like
        try:
            return self.library.get_schema(schema_like)
        except KeyError:
            schema = self.get_generated_schema(schema_like)
            if schema is None:
                raise KeyError(schema_like)
            return schema

    def add_schema(self, schema: ObjectSchema):
        self.library.add_schema(schema)

    def get_generated_schema(
        self, schema_like: ObjectSchemaLike
    ) -> Optional[ObjectSchema]:
        if isinstance(schema_like, str):
            key = schema_like
        elif isinstance(schema_like, ObjectSchema):
            key = schema_like.key
        else:
            raise TypeError(schema_like)
        with self.session_scope() as sess:
            got = sess.query(GeneratedObjectSchema).get(key)
            if got is None:
                return None
            return got.as_schema()

    def add_new_generated_schema(self, schema: ObjectSchema, sess: Session):
        if schema.key in self.library.schemas:
            # Already exists
            return
        got = GeneratedObjectSchema(key=schema.key, definition=asdict(schema))
        sess.add(got)
        self.library.add_schema(schema)

    def all_schemas(self) -> List[ObjectSchema]:
        return self.library.all_schemas()

    def get_pipe(self, pipe_like: str) -> Pipe:
        return self.library.get_pipe(pipe_like)

    def add_pipe(self, pipe: Pipe):
        self.library.add_pipe(pipe)

    def all_pipes(self) -> List[Pipe]:
        return self.library.all_pipes()

    def add_module(self, module: DagsModule):
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
        from dags.core.storage.storage import StorageClass

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
        self, graph: Graph, session: Session, target_storage: Storage = None, **kwargs
    ) -> ExecutionContext:
        from dags.core.storage.storage import StorageClass
        from dags.core.runnable import ExecutionContext

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
            metadata_session=session,
            runtimes=self.runtimes,
            storages=self.storages,
            target_storage=target_storage,
            local_memory_storage=self.get_default_local_memory_storage(),
        )
        args.update(**kwargs)
        return ExecutionContext(**args)  # type: ignore

    @contextmanager
    def execution(self, graph: Graph, target_storage: Storage = None, **kwargs):
        from dags.core.runnable import ExecutionManager

        session = self.Session()
        ec = self.get_execution_context(
            graph, session, target_storage=target_storage, **kwargs
        )
        em = ExecutionManager(ec)
        try:
            yield em
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            # TODO:
            # self.validate_and_clean_data_blocks(delete_intermediate=True)
            session.close()

    def produce(
        self,
        graph: Graph,
        node_like: Optional[Union[Node, str]] = None,
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ) -> Optional[DataBlock]:
        from dags.core.node import Node

        if node_like is not None:
            node = node_like
            if isinstance(node, str):
                node = graph.get_any_node(node_like)
            assert isinstance(node, Node)
            dependencies = graph.get_declared_graph_with_dataset_nodes().get_all_upstream_dependencies_in_execution_order(
                node
            )
        else:
            dependencies = (
                graph.get_declared_graph_with_dataset_nodes().get_all_nodes_in_execution_order()
            )
        output = None
        for dep in dependencies:
            with self.execution(graph, **execution_kwargs) as em:
                output = em.run(dep, to_exhaustion=to_exhaustion)
        return output

    def run_node(
        self,
        graph: Graph,
        node_like: Union[Node, str],
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ) -> Optional[DataBlock]:
        from dags.core.node import Node

        if isinstance(node_like, str):
            node_like = graph.get_any_node(node_like)
        assert isinstance(node_like, Node)

        logger.debug(f"Running: {node_like}")
        with self.execution(graph, **execution_kwargs) as em:
            return em.run(node_like, to_exhaustion=to_exhaustion)

    def run_graph(self, graph, to_exhaustion: bool = True, **execution_kwargs: Any):
        nodes = (
            graph.get_declared_graph_with_dataset_nodes().get_all_nodes_in_execution_order()
        )
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
        from dags.core.storage.storage import Storage

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
            from dags.core.runtime import Runtime

            try:
                rt = Runtime.from_storage(sr)
                self.runtimes.append(rt)
            except ValueError:
                pass
        return sr

    def validate_and_clean_data_blocks(
        self, delete_memory=True, delete_intermediate=False, force: bool = False
    ):
        with self.session_scope() as sess:
            from dags.core.data_block import (
                DataBlockMetadata,
                StoredDataBlockMetadata,
            )

            if delete_memory:
                deleted = (
                    sess.query(StoredDataBlockMetadata)
                    .filter(StoredDataBlockMetadata.storage_url.startswith("memory:"))
                    .delete(False)
                )
                print(f"{deleted} Memory StoredDataBlocks deleted")

            for block in sess.query(DataBlockMetadata).filter(
                ~DataBlockMetadata.stored_data_blocks.any()
            ):
                print(f"#{block.id} {block.expected_schema_key} is orphaned! SAD")
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
                    sess.query(DataBlockMetadata)
                    .filter(
                        ~DataBlockMetadata.data_sets.any(),
                    )
                    .update(
                        {DataBlockMetadata.deleted: True}, synchronize_session=False
                    )
                )
                print(f"{cnt} intermediate DataBlocks deleted")

    def add_event_handler(self, eh: EventHandler):
        self.event_handlers.append(eh)

    def send_event(
        self, event_subject: Union[EventSubject, str], event_details, **kwargs
    ) -> Event:
        e = event_factory(event_subject, event_details, **kwargs)
        for handler in self.event_handlers:
            handler.handle(e)
        return e


# Not supporting yml project config atm
# def load_environment_from_yaml(yml) -> Environment:
#     from dags.core.storage.storage import StorageResource
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
    from dags.core.storage.storage import Storage

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
