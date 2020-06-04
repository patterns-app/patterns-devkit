from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from importlib import import_module
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Union

from sqlalchemy.orm import Session, sessionmaker

from basis.core.metadata.orm import BaseModel
from basis.core.module import BasisModule
from basis.core.registries import DataFunctionRegistry, ObjectTypeRegistry
from basis.core.typing.object_type import ObjectType, ObjectTypeLike
from basis.utils.registry import Registry, UriRegistry
from basis.utils.uri import DEFAULT_MODULE_KEY

if TYPE_CHECKING:
    from basis.core.storage import (
        Storage,
        new_local_memory_storage,
    )
    from basis.core.data_function import DataFunctionCallable
    from basis.core.function_node import FunctionNode, function_node_factory
    from basis.core.data_function_interface import FunctionGraphResolver
    from basis.indexing.components import IndexableComponent
    from basis.core.runnable import ExecutionContext
    from basis.core.data_block import DataBlock

logger = logging.getLogger(__name__)


class Environment:
    otype_registry: UriRegistry
    storages: List[Storage]
    metadata_storage: Storage

    def __init__(
        self,
        key: str = None,
        metadata_storage: Union["Storage", str] = None,
        otype_registry: ObjectTypeRegistry = None,
        data_function_registry: DataFunctionRegistry = None,
        provider_registry: Registry = None,
        # TODO: SourceResource registry too?
        module_registry: Registry = None,
        create_metadata_storage: bool = True,
        add_default_python_runtime: bool = True,
    ):
        from basis.core.runtime import Runtime
        from basis.core.runtime import RuntimeClass
        from basis.core.runtime import RuntimeEngine
        from basis.core.storage import Storage

        self.key = key

        if metadata_storage is None and create_metadata_storage:

            # TODO: kind of hidden. make configurable at least, and log/print to user
            metadata_storage = Storage.from_url("sqlite:///.basis_metadata.db")
        if isinstance(metadata_storage, str):
            metadata_storage = Storage.from_url(metadata_storage)
        if metadata_storage is None:
            raise Exception("Must specify metadata_storage or allow default")
        self.metadata_storage = metadata_storage
        # if create_metadata_storage: # TODO: hmmm
        self.initialize_metadata_database()
        self.module_registry = module_registry or Registry()
        self.otype_registry = otype_registry or ObjectTypeRegistry()
        self.data_function_registry = data_function_registry or DataFunctionRegistry()
        self.provider_registry = provider_registry or Registry()
        self.added_nodes: Registry = Registry()
        self._flattened_nodes: Registry = Registry()
        self.storages = []
        self.runtimes = []
        if add_default_python_runtime:
            self.runtimes.append(
                Runtime(
                    url="python://local",
                    runtime_class=RuntimeClass.PYTHON,
                    runtime_engine=RuntimeEngine.LOCAL,
                )
            )
        self._module_order: List[str] = []

    def initialize_metadata_database(self):
        from basis.core.metadata.listeners import add_persisting_sdb_listener

        conn = self.metadata_storage.get_database_api(self).get_connection()
        BaseModel.metadata.create_all(conn)
        self.Session = sessionmaker(bind=conn)
        add_persisting_sdb_listener(self.Session)
        self._env_session = self.Session()

    def get_env_metadata_session(self) -> Session:
        return self._env_session

    def get_new_metadata_session(self) -> Session:
        # return self.Session()
        return self.Session()

    def get_module_order(self) -> List[str]:
        return [DEFAULT_MODULE_KEY] + self._module_order

    def get_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        if isinstance(otype_like, ObjectType):
            return otype_like
        return self.otype_registry.get(
            otype_like, module_precedence=self.get_module_order()
        )

    def add_node(
        self, _key: str, _data_function: DataFunctionCallable, **kwargs
    ) -> FunctionNode:
        from basis.core.function_node import function_node_factory

        node = function_node_factory(self, _key, _data_function, **kwargs)
        self.added_nodes.register(node)
        self.register_node(node)
        return node

    def register_node(self, node: "FunctionNode"):
        if node.is_composite():
            for sub_node in node.get_nodes():
                self.register_node(sub_node)
        else:
            self._flattened_nodes.register(node)

    def all_added_nodes(self) -> List[FunctionNode]:
        return list(self.added_nodes.all())

    def flattened_nodes(self) -> List[FunctionNode]:
        return list(self._flattened_nodes.all())

    def get_node(self, node_like: Union["FunctionNode", str]) -> "FunctionNode":
        from basis.core.function_node import FunctionNode

        if isinstance(node_like, FunctionNode):
            return node_like
        try:
            return self.added_nodes.get(node_like)
        except KeyError:  # TODO: do we want to get flattened (sub) nodes too? Probably
            return self._flattened_nodes.get(node_like)

    def get_function_graph_resolver(self) -> FunctionGraphResolver:
        from basis.core.data_function_interface import FunctionGraphResolver

        return FunctionGraphResolver(self)  # TODO: maybe cache this?

    def add_module(self, module: BasisModule):
        self.module_registry.register(module)
        self.otype_registry.merge(module.otypes)
        self.provider_registry.merge(module.providers)
        self._module_order.append(module.key)

    def get_indexable_components(self) -> Iterable[IndexableComponent]:
        for module in self.module_registry.all():
            for c in module.get_indexable_components():
                yield c

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

    def get_execution_context(self, session: Session, **kwargs) -> ExecutionContext:
        from basis.core.runnable import ExecutionContext
        from basis.core.storage import new_local_memory_storage

        target_storage = self.storages[0] if self.storages else None
        args = dict(
            env=self,
            metadata_session=session,
            runtimes=self.runtimes,
            storages=self.storages,
            target_storage=target_storage,
            local_memory_storage=new_local_memory_storage(),
        )
        args.update(**kwargs)
        return ExecutionContext(**args)

    @contextmanager
    def execution(self, target_storage: Storage = None):
        # TODO: target storage??
        from basis.core.runnable import ExecutionManager

        session = self.Session()
        ec = self.get_execution_context(session)
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
        self, node_like: Union[FunctionNode, str], **execution_kwargs: Any
    ) -> Optional[DataBlock]:
        from basis.core.function_node import FunctionNode

        fgr = self.get_function_graph_resolver()

        if isinstance(node_like, str):
            node_like = self.get_node(node_like)
        assert isinstance(node_like, FunctionNode)
        output_node = node_like.get_output_node()  # Handle composite functions
        dependencies = fgr.get_all_upstream_dependencies_in_execution_order(output_node)
        output = None
        for dep in dependencies:
            with self.execution(**execution_kwargs) as em:
                output = em.run(dep, to_exhaustion=True)
        return output

    def update_all(self, to_exhaustion: bool = True, **execution_kwargs: Any):
        fgr = self.get_function_graph_resolver()
        nodes = fgr.get_all_nodes_in_execution_order()
        for node in nodes:
            with self.execution(**execution_kwargs) as em:
                em.run(node, to_exhaustion=to_exhaustion)

    def get_latest_output(self, node: FunctionNode) -> Optional[DataBlock]:
        session = self.get_new_metadata_session()  # TODO: hanging session
        ctx = self.get_execution_context(session)
        return node.get_latest_output(ctx)

    def add_storage(
        self, storage_like: Union[Storage, str], add_runtime=True
    ) -> Storage:
        from basis.core.storage import Storage

        if isinstance(storage_like, str):
            sr = Storage.from_url(storage_like)
        elif isinstance(storage_like, Storage):
            sr = storage_like
        else:
            raise TypeError
        self.storages.append(sr)
        if add_runtime:
            from basis.core.runtime import Runtime

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
            from basis.core.data_block import (
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
                print(f"#{block.id} {block.otype_uri} is orphaned! SAD")
            if delete_intermediate:
                # TODO: does no checking if they are unprocessed or not...
                if not force:
                    d = input(
                        "Are you sure you want to delete ALL intermediate DataBlocks? There is no undoing this operation. y/N?"
                    )
                    if not d or d.lower()[0] != "y":
                        return
                # Delete DRs with no DataSet
                cnt = (
                    sess.query(DataBlockMetadata)
                    .filter(~DataBlockMetadata.data_sets.any(),)
                    .update(
                        {DataBlockMetadata.deleted: True}, synchronize_session=False
                    )
                )
                print(f"{cnt} intermediate DataBlocks deleted")


# Not supporting yml project config atm
# def load_environment_from_yaml(yml) -> Environment:
#     from basis.core.storage import StorageResource
#
#     env = Environment(
#         metadata_storage=yml.get("metadata_storage", None),
#         add_default_python_runtime=yml.get("add_default_python_runtime", True),
#     )
#     for url in yml.get("storages"):
#         env.add_storage(StorageResource.from_url(url))
#     for module_name in yml.get("modules"):
#         m = import_module(module_name)
#         env.add_module(m)
#     return env


def load_environment_from_project(project: Any) -> Environment:
    from basis.core.storage import Storage

    env = Environment(
        metadata_storage=getattr(project, "metadata_storage", None),
        add_default_python_runtime=getattr(project, "add_default_python_runtime", True),
    )
    for url in getattr(project, "storages", []):
        env.add_storage(Storage.from_url(url))
    for module_name in getattr(project, "modules", []):
        m = import_module(module_name)
        env.add_module(m)  # type: ignore  # We hijack the module
    return env


def current_env(cfg_module: str = "project") -> Environment:
    import sys

    sys.path.append(os.getcwd())
    cfg = import_module(cfg_module)
    return load_environment_from_project(cfg)
