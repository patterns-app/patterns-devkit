from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union

from alembic import command
from alembic.config import Config
from commonmodel.base import Schema, SchemaLike
from dcp import Storage
from dcp.storage.base import MemoryStorageClass, ensure_storage
from dcp.storage.memory.engines.python import new_local_python_storage
from dcp.utils.common import AttrDict
from loguru import logger
from snapflow.core.component import ComponentLibrary
from snapflow.core.metadata.api import MetadataApi
from snapflow.core.metadata.orm import BaseModel
from snapflow.core.module import (
    DEFAULT_LOCAL_MODULE,
    DEFAULT_LOCAL_NAMESPACE,
    SnapflowModule,
)
from snapflow.core.schema import GeneratedSchema, GenericSchemaException, is_generic
from sqlalchemy import select
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from snapflow.core.function import DataFunction
    from snapflow.core.node import Node, NodeLike
    from snapflow.core.data_block import DataBlock
    from snapflow.core.graph import Graph, DeclaredGraph, DEFAULT_GRAPH
    from snapflow.core.runtime import Runtime, LocalPythonRuntimeEngine
    from snapflow.core.execution import ExecutionResult, Executable, ExecutionContext
    from snapflow.core.execution.executable import CumulativeExecutionResult

DEFAULT_METADATA_STORAGE_URL = "sqlite://"  # in-memory sqlite


Serializable = Union[str, int, float, bool]


@dataclass(frozen=True)
class SnapflowSettings:
    initialize_metadata_storage: bool = True
    abort_on_function_error: bool = False
    execution_timelimit_seconds: Optional[int] = None
    fail_on_downcast: bool = False
    warn_on_downcast: bool = True
    add_core_module: bool = True


@dataclass(frozen=True)
class EnvironmentConfiguration:
    key: str = "default"
    metadata_storage_url: Optional[str] = None
    # modules: List[SnapflowModule] = field(default_factory=list)
    namespaces: List[str] = field(default_factory=list)
    default_storage_url: Optional[str] = None
    storage_urls: List[str] = field(default_factory=list)
    runtime_urls: List[str] = field(default_factory=list)
    settings: Optional[SnapflowSettings] = None


class Environment:
    library: ComponentLibrary

    def __init__(
        self,
        key: str = "default",
        metadata_storage: Union["Storage", str] = None,
        modules: List[Union[SnapflowModule, str]] = None,  # Defaults to `core` module
        storages: List[Union[Storage, str]] = None,
        default_storage: Union[Storage, str] = None,
        runtimes: List[Union[Runtime, str]] = None,
        settings: SnapflowSettings = None,
        config: Optional[EnvironmentConfiguration] = None,
    ):
        from snapflow.modules import core
        from snapflow.core.runtime import ensure_runtime

        self.key = key
        # if self.key in environments:
        #     raise NameError(f"Environment {self.key} already exists")
        self.storages = [ensure_storage(s) for s in storages or []]
        self.runtimes = [ensure_runtime(s) for s in runtimes or []]
        self.settings = settings or SnapflowSettings()
        self.config = config
        if metadata_storage is None:
            metadata_storage = DEFAULT_METADATA_STORAGE_URL
            logger.warning(
                f"No metadata storage specified, using default sqlite db `{DEFAULT_METADATA_STORAGE_URL}`"
            )
        self.metadata_storage = ensure_storage(metadata_storage)
        self.metadata_api = MetadataApi(self.key, self.metadata_storage)
        if self.settings.initialize_metadata_storage:
            self.metadata_api.initialize_metadata_database()
        # TODO: local module is yucky global state, also, we load these libraries and their
        #       components once, but the libraries are mutable and someone could add components
        #       to them later, which would not be picked up by the env library. (prob fine)
        self._local_module = DEFAULT_LOCAL_MODULE
        self.default_storage = ensure_storage(default_storage)
        # TODO: load library from config
        self.library = ComponentLibrary()
        self.add_module(self._local_module)
        if self.settings.add_core_module:
            self.add_module(core)
        for m in modules or []:
            self.add_module(m)

        self._local_python_storage = new_local_python_storage()
        self.add_storage(self._local_python_storage)
        # get_environment(self)

    @staticmethod
    def from_config(cfg: EnvironmentConfiguration):
        # if cfg.key in environments:
        #     return get_environment(cfg.key)
        env = Environment(
            key=cfg.key,
            metadata_storage=cfg.metadata_storage_url,
            modules=cfg.namespaces,
            storages=cfg.storage_urls,
            default_storage=cfg.default_storage_url,
            runtimes=cfg.runtime_urls,
            settings=cfg.settings,
            config=cfg,
        )
        return env
        # return get_environment(env)

    def get_metadata_api(self) -> MetadataApi:
        return self.metadata_api

    def as_config(self) -> EnvironmentConfiguration:
        return EnvironmentConfiguration(
            key=self.key,
            metadata_storage_url=self.metadata_storage.url,
            namespaces=self.get_namespaces(),  # TODO: check if these are importable, raise if not
            default_storage_url=self.get_default_storage().url,
            storage_urls=[s.url for s in self.storages],
            runtime_urls=[s.url for s in self.runtimes],
            settings=self.settings,
        )

    def get_namespaces(self) -> List[str]:
        return self.library.module_lookup_names

    # Shortcut
    @property
    def md_api(self) -> MetadataApi:
        return self.get_metadata_api()

    def get_default_local_python_storage(self) -> Storage:
        return self._local_python_storage

    def get_local_module(self) -> SnapflowModule:
        return self._local_module

    def get_module_order(self) -> List[str]:
        return self.library.module_lookup_names

    def get_schema(self, schema_like: SchemaLike) -> Schema:
        if is_generic(schema_like):
            raise GenericSchemaException("Cannot get generic schema `{schema_like}`")
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
        got = self.md_api.execute(
            select(GeneratedSchema).filter(GeneratedSchema.key == key)
        ).scalar_one_or_none()
        if got is None:
            return None
        return got.as_schema()

    def add_new_generated_schema(self, schema: Schema):
        logger.debug(f"Adding new generated schema {schema}")
        if schema.key in self.library.schemas:
            # Already exists
            return
        got = GeneratedSchema(key=schema.key, definition=asdict(schema))
        self.md_api.add(got)
        self.md_api.flush([got])
        self.library.add_schema(schema)

    def all_schemas(self) -> List[Schema]:
        return self.library.all_schemas()

    def get_function(self, function_like: str) -> DataFunction:
        return self.library.get_function(function_like)

    def add_function(self, function: DataFunction):
        self.library.add_function(function)

    def all_functions(self) -> List[DataFunction]:
        return self.library.all_functions()

    def add_module(self, *modules: Union[SnapflowModule, ModuleType, str]):
        for module in modules:
            if isinstance(module, str):
                if module in (DEFAULT_LOCAL_NAMESPACE, "core"):
                    continue
                try:
                    module = import_module(module)
                except ImportError:
                    if "test" in module:
                        logger.debug(f"Could not import module {module}")
                    else:
                        logger.warning(f"Could not import module {module}")
                    continue
            self.library.add_module(module)

    def get_default_storage(self) -> Storage:
        if self.default_storage is not None:
            return self.default_storage
        if len(self.storages) == 1:
            return self.storages[0]
        for s in self.storages:
            if s.url == self.metadata_storage.url:
                continue
            if s.storage_engine.storage_class == MemoryStorageClass:
                continue
            return s
        return self.storages[0]

    def get_execution_context(
        self, target_storage: Storage = None, **kwargs
    ) -> ExecutionContext:
        from snapflow.core.execution import ExecutionContext, ExecutionConfiguration

        if target_storage is None:
            target_storage = self.get_default_storage()
        target_storage = self.add_storage(target_storage)
        if issubclass(target_storage.storage_engine.storage_class, MemoryStorageClass):
            # TODO: handle multiple targets better
            logging.warning(
                "Using MEMORY storage -- results of execution will NOT "
                "be persisted. Add a database or file storage to persist results."
            )
        args = dict(
            env=self,
            local_storage=self._local_python_storage,
            target_storage=target_storage,
            storages=self.storages,
            abort_on_function_error=self.settings.abort_on_function_error,
        )
        args.update(**kwargs)
        return ExecutionContext(**args)

    # @contextmanager
    # def run(
    #     self, graph: Graph, target_storage: Storage = None, **kwargs
    # ) -> Iterator[ExecutionManager]:
    #     from snapflow.core.execution import ExecutionManager

    #     # self.session.begin_nested()
    #     ec = self.get_execution_context(target_storage=target_storage, **kwargs)
    #     em = ExecutionManager(ec)
    #     logger.debug(f"executing on graph {graph.adjacency_list()}")
    #     try:
    #         yield em
    #     except Exception as e:
    #         raise e
    #     finally:
    #         # TODO:
    #         # self.validate_and_clean_data_blocks(delete_intermediate=True)
    #         pass

    def get_executable(
        self, node: Node, target_storage: Storage = None, **kwargs
    ) -> Executable:
        from snapflow.core.execution import Executable

        return Executable(
            node=node,
            function=node.function,
            execution_context=self.get_execution_context(
                target_storage=target_storage, **kwargs
            ),
        )

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
            assert isinstance(node, Node), node
        assert isinstance(graph, Graph)
        return node, graph

    def produce(
        self,
        node_like: Optional[NodeLike] = None,
        graph: Union[Graph, DeclaredGraph] = None,
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ) -> List[DataBlock]:
        from snapflow.core.execution import execute_to_exhaustion

        node, graph = self._get_graph_and_node(node_like, graph)
        if node is not None:
            dependencies = graph.get_all_upstream_dependencies_in_execution_order(node)
        else:
            dependencies = graph.get_all_nodes_in_execution_order()
        result = None
        for dep in dependencies:
            result = execute_to_exhaustion(
                self.get_executable(dep, **execution_kwargs),
                to_exhaustion=to_exhaustion,
            )
        if result:
            with self.metadata_api.begin():
                return result.get_output_blocks(self)
        return []

    def run_node(
        self,
        node_like: Optional[NodeLike] = None,
        graph: Union[Graph, DeclaredGraph] = None,
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ) -> Optional[CumulativeExecutionResult]:
        from snapflow.core.execution import execute_to_exhaustion

        node, graph = self._get_graph_and_node(node_like, graph)
        assert node is not None

        logger.debug(f"Running: {node_like}")
        result = execute_to_exhaustion(
            self.get_executable(node, **execution_kwargs), to_exhaustion=to_exhaustion
        )
        return result

    def run_graph(
        self,
        graph: Union[Graph, DeclaredGraph],
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ):
        from snapflow.core.execution import execute_to_exhaustion
        from snapflow.core.graph import DeclaredGraph

        if isinstance(graph, DeclaredGraph):
            graph = graph.instantiate(self)
        nodes = graph.get_all_nodes_in_execution_order()
        for node in nodes:
            execute_to_exhaustion(
                self.get_executable(node, **execution_kwargs),
                to_exhaustion=to_exhaustion,
            )

    def get_latest_output(
        self, node: NodeLike, graph: Union[Graph, DeclaredGraph] = None
    ) -> Optional[DataBlock]:
        with self.metadata_api.begin():
            n, graph = self._get_graph_and_node(node, graph)
            return n.latest_output(self)

    def add_storage(
        self, storage_like: Union[Storage, str], add_runtime: bool = True
    ) -> Storage:

        if isinstance(storage_like, str):
            sr = Storage.from_url(storage_like)
        elif isinstance(storage_like, Storage):
            sr = storage_like
        else:
            raise TypeError
        if sr.url not in [s.url for s in self.storages]:
            self.storages.append(sr)
        if add_runtime:
            from snapflow.core.runtime import Runtime

            try:
                rt = Runtime.from_storage(sr)
                self.add_runtime(rt)
            except ValueError:
                pass
        return sr

    def add_runtime(self, runtime_like: Union[Runtime, str]) -> Runtime:
        from snapflow.core.runtime import Runtime

        if isinstance(runtime_like, str):
            sr = Runtime.from_url(runtime_like)
        elif isinstance(runtime_like, Runtime):
            sr = runtime_like
        else:
            raise TypeError
        if sr.url not in [s.url for s in self.runtimes]:
            self.runtimes.append(sr)
        return sr

    # def serialize_run_node(
    #     self,
    #     node_like: Optional[NodeLike] = None,
    #     graph: Union[Graph, DeclaredGraph] = None,
    #     to_exhaustion: bool = True,
    #     **execution_kwargs: Any,
    # ) -> Optional[DataBlock]:
    #     node, graph = self._get_graph_and_node(node_like, graph)
    #     assert node is not None

    #     logger.debug(f"Running: {node_like}")
    #     sess = self._get_new_metadata_session()  # hanging session
    #     with self.run(graph, **execution_kwargs) as em:
    #         return em.execute(node, to_exhaustion=to_exhaustion, output_session=sess)

    # TODO
    # def validate_and_clean_data_blocks(
    #     self, delete_memory=True, delete_intermediate=False, force: bool = False
    # ):
    #     from snapflow.core.data_block import (
    #         DataBlockMetadata,
    #         StoredDataBlockMetadata,
    #     )

    #     if delete_memory:
    #         deleted = (
    #             self.session.query(StoredDataBlockMetadata)
    #             .filter(StoredDataBlockMetadata.storage_url.startswith("memory:"))
    #             .delete(False)
    #         )
    #         print(f"{deleted} Memory StoredDataBlocks deleted")

    #     for block in self.session.query(DataBlockMetadata).filter(
    #         ~DataBlockMetadata.stored_data_blocks.any()
    #     ):
    #         print(f"#{block.id} {block.nominal_schema_key} is orphaned! SAD")
    #     if delete_intermediate:
    #         # TODO: does no checking if they are unprocessed or not...
    #         if not force:
    #             d = input(
    #                 "Are you sure you want to delete ALL intermediate DataBlocks? There is no undoing this operation. y/N?"
    #             )
    #             if not d or d.lower()[0] != "y":
    #                 return
    #         # Delete blocks with no DataSet
    #         cnt = (
    #             self.session.query(DataBlockMetadata)
    #             .filter(~DataBlockMetadata.data_sets.any(),)
    #             .update({DataBlockMetadata.deleted: True}, synchronize_session=False)
    #         )
    #         print(f"{cnt} intermediate DataBlocks deleted")


# Shortcuts
def produce(
    *args,
    env: Optional[Environment] = None,
    modules: Optional[List[SnapflowModule]] = None,
    **kwargs: Any,
) -> List[DataBlock]:
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
) -> Optional[CumulativeExecutionResult]:
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


def run(
    node_or_graph: Union[NodeLike, DeclaredGraph, Graph], *args, **kwargs
) -> Optional[CumulativeExecutionResult]:
    from snapflow.core.graph import Graph, DeclaredGraph

    if isinstance(node_or_graph, Graph) or isinstance(node_or_graph, DeclaredGraph):
        return run_graph(node_or_graph, *args, **kwargs)
    return run_node(node_or_graph, *args, **kwargs)


### Environments are singletons!

# environments: Dict[str, Environment] = {}

# def get_environment(env_or_name: Union[str, Environment]) -> Environment:
#     env = None
#     if isinstance(env_or_name, Environment):
#         name = env_or_name.name
#         env = env_or_name
#     else:
#         name = env_or_name
#     if not name in environments:
#         if env is None:
#             raise KeyError(name)
#         environments[name] = env
#     return environments[name]

# def load_environment_from_yaml(yml) -> Environment:
#

#     env = Environment(
#         metadata_storage=yml.get("metadata_storage", None),
#         add_default_python_runtime=yml.get("add_default_python_runtime", True),
#     )
#     for url in yml.get("storages", []):
#         env.add_storage(Storage.from_url(url))
#     for namespace in yml.get("modules", []):
#         m = import_module(namespace)
#         env.add_module(m)
#     return env


def load_environment_from_project(project: Any) -> Environment:

    env = Environment(
        metadata_storage=getattr(project, "metadata_storage", None),
        add_default_python_runtime=getattr(project, "add_default_python_runtime", True),
    )
    for url in getattr(project, "storages", []):
        env.add_storage(Storage.from_url(url))
    for namespace in getattr(project, "modules", []):
        m = import_module(namespace)
        env.add_module(m)  # type: ignore  # We hijack the module
    return env


def current_env(cfg_module: str = None) -> Optional[Environment]:
    import sys
    from snapflow.project.project import SNAPFLOW_PROJECT_PACKAGE_NAME

    if cfg_module is None:
        cfg_module = SNAPFLOW_PROJECT_PACKAGE_NAME
    sys.path.append(os.getcwd())
    try:
        cfg = import_module(cfg_module)
        return load_environment_from_project(cfg)
    except ImportError:
        pass
    # with open(cfg_file) as f:
    #     yml = strictyaml.load(f.read()).data
    # return load_environment_from_yaml(yml)
    return None
