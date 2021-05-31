from __future__ import annotations

import logging
import os
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
from snapflow.core.component import ComponentLibrary, global_library
from snapflow.core.metadata.api import MetadataApi
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
    from snapflow.core.data_block import DataBlock
    from snapflow.core.declarative.graph import GraphCfg
    from snapflow.core.declarative.execution import ExecutableCfg, ExecutionCfg
    from snapflow.core.declarative.dataspace import DataspaceCfg, SnapflowCfg
    from snapflow.core.declarative.execution import CumulativeExecutionResult

DEFAULT_METADATA_STORAGE_URL = "sqlite://"  # in-memory sqlite


Serializable = Union[str, int, float, bool]


class Environment:
    key: str
    dataspace: DataspaceCfg
    library: ComponentLibrary
    settings: SnapflowCfg
    metadata_storage: Storage
    metadata_api: MetadataApi

    def __init__(
        self,
        dataspace: Optional[DataspaceCfg] = None,
        library: Optional[ComponentLibrary] = global_library,
        namespaces: List[str] = None,
    ):
        from snapflow.modules import core
        from snapflow.core.runtime import ensure_runtime
        from snapflow.core.declarative.dataspace import DataspaceCfg, SnapflowCfg

        self.dataspace = dataspace or DataspaceCfg()
        self.key = self.dataspace.key or "default"
        self.settings = self.dataspace.snapflow or SnapflowCfg()
        metadata_storage = self.dataspace.metadata_storage
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
        # TODO: load library from config
        if library is None or not self.settings.use_global_library:
            self.library = ComponentLibrary()
        else:
            self.library = library
        self.add_module(self._local_module)
        self._local_python_storage = new_local_python_storage()
        # self.add_storage(self._local_python_storage)

    def get_metadata_api(self) -> MetadataApi:
        return self.metadata_api

    def get_namespaces(self) -> List[str]:
        return self.library.namespace_precedence

    # Shortcut
    @property
    def md_api(self) -> MetadataApi:
        return self.get_metadata_api()

    def get_default_local_python_storage(self) -> Storage:
        return self._local_python_storage

    def get_local_module(self) -> SnapflowModule:
        return self._local_module

    def get_module_order(self) -> List[str]:
        return self.library.namespace_precedence

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
                raise KeyError(
                    f"Schema '{schema_like}' not found (available namespaces: {self.library.namespace_precedence})"
                )
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
        got = GeneratedSchema(key=schema.key, definition=schema.dict())
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
        if self.dataspace.default_storage is not None:
            return ensure_storage(self.dataspace.default_storage)
        if len(self.dataspace.storages) == 1:
            return ensure_storage(self.dataspace.storages[0])
        if not self.dataspace.storages:
            return self._local_python_storage
        for s in self.dataspace.storages:
            s = ensure_storage(s)
            if s.url == self.metadata_storage.url:
                continue
            if s.storage_engine.storage_class == MemoryStorageClass:
                continue
            return s
        return ensure_storage(self.dataspace.storages[0])

    def get_execution_config(
        self, target_storage: Union[Storage, str] = None, **kwargs
    ) -> ExecutionCfg:
        from snapflow.core.declarative.execution import ExecutionCfg

        if target_storage is None:
            target_storage = self.get_default_storage()
        target_storage = ensure_storage(target_storage)
        # target_storage = self.add_storage(target_storage)
        if issubclass(target_storage.storage_engine.storage_class, MemoryStorageClass):
            # TODO: handle multiple targets better
            logging.warning(
                "Using MEMORY storage -- results of execution will NOT "
                "be persisted. Add a database or file storage to persist results."
            )
        args = dict(
            dataspace=self.dataspace,
            local_storage=self._local_python_storage.url,
            target_storage=target_storage.url,
            storages=[s.url for s in self.get_storages()] + [target_storage.url]
            # abort_on_function_error=self.settings.abort_on_function_error,
        )
        args.update(**kwargs)
        return ExecutionCfg(**args)

    def get_storages(self) -> List[Storage]:
        return [Storage(s) for s in self.dataspace.storages] + [
            self._local_python_storage
        ]

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

    def prepare_graph(self, graph: Optional[GraphCfg] = None) -> GraphCfg:
        if graph is None:
            graph = self.dataspace.graph
        graph = graph.resolve_and_flatten(self.library)
        return graph

    def get_executable(
        self,
        node: GraphCfg,
        graph: Optional[GraphCfg] = None,
        target_storage: Union[Storage, str] = None,
        **kwargs,
    ) -> ExecutableCfg:
        from snapflow.core.declarative.execution import ExecutableCfg

        graph = self.prepare_graph(graph)
        return ExecutableCfg(
            node_key=node.key,
            graph=graph,
            execution_config=self.get_execution_config(
                target_storage=target_storage, **kwargs
            ),
        )

    def produce(
        self,
        node: Union[GraphCfg, str] = None,
        graph: Optional[GraphCfg] = None,
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ) -> List[DataBlock]:
        from snapflow.core.execution import execute_to_exhaustion

        graph = self.prepare_graph(graph)
        if isinstance(node, str):
            node = graph.get_node(node)
        assert node.is_function_node()

        if node is not None:
            dependencies = graph.get_all_upstream_dependencies_in_execution_order(node)
        else:
            dependencies = graph.get_all_nodes_in_execution_order()
        result = None
        for dep in dependencies:
            result = execute_to_exhaustion(
                self,
                self.get_executable(dep, graph=graph, **execution_kwargs),
                to_exhaustion=to_exhaustion,
            )
        if result:
            with self.metadata_api.begin():
                return result.get_output_blocks(self)
        return []

    def run_node(
        self,
        node: Union[GraphCfg, str],
        graph: Optional[GraphCfg] = None,
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ) -> Optional[CumulativeExecutionResult]:
        from snapflow.core.execution import execute_to_exhaustion

        graph = self.prepare_graph(graph)
        logger.debug(f"Running: {node}")
        node = graph.get_node(node)
        node.resolve(self.library)
        result = execute_to_exhaustion(
            self,
            self.get_executable(node, graph=graph, **execution_kwargs),
            to_exhaustion=to_exhaustion,
        )
        return result

    def run_graph(
        self,
        graph: Optional[GraphCfg] = None,
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
    ):
        from snapflow.core.execution import execute_to_exhaustion

        graph = self.prepare_graph(graph)
        nodes = graph.get_all_nodes_in_execution_order()
        for node in nodes:
            execute_to_exhaustion(
                self,
                self.get_executable(node, graph=graph, **execution_kwargs),
                to_exhaustion=to_exhaustion,
            )

    def get_latest_output(self, node: GraphCfg) -> Optional[DataBlock]:
        from snapflow.core.execution import get_latest_output

        return get_latest_output(self, node)


# Shortcuts
def produce(
    node: Union[str, GraphCfg],
    graph: Optional[GraphCfg] = None,
    env: Optional[Environment] = None,
    modules: Optional[List[SnapflowModule]] = None,
    **kwargs: Any,
) -> List[DataBlock]:
    if env is None:
        env = Environment()
    if modules is not None:
        for module in modules:
            env.add_module(module)
    return env.produce(node, graph=graph, **kwargs)


def run_node(
    node: Union[str, GraphCfg],
    graph: Optional[GraphCfg] = None,
    env: Optional[Environment] = None,
    modules: Optional[List[SnapflowModule]] = None,
    **kwargs: Any,
) -> Optional[CumulativeExecutionResult]:
    if env is None:
        env = Environment()
    if modules is not None:
        for module in modules:
            env.add_module(module)
    return env.run_node(node, graph=graph, **kwargs)


def run_graph(
    graph: GraphCfg,
    env: Optional[Environment] = None,
    modules: Optional[List[SnapflowModule]] = None,
    **kwargs: Any,
):
    if env is None:
        env = Environment()
    if modules is not None:
        for module in modules:
            env.add_module(module)
    return env.run_graph(graph, **kwargs)


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
