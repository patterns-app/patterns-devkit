from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import asdict
from importlib import import_module
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Union

from sqlalchemy.orm import Session, sessionmaker

from basis.core.component import ComponentLibrary, ensure_uri
from basis.core.metadata.orm import BaseModel
from basis.core.module import DEFAULT_LOCAL_MODULE, BasisModule
from basis.core.typing.object_type import (
    GeneratedObjectType,
    ObjectType,
    ObjectTypeLike,
)

if TYPE_CHECKING:
    from basis.core.streams import FunctionNodeRawInput
    from basis.core.storage.storage import (
        Storage,
        new_local_memory_storage,
        StorageClass,
    )
    from basis.core.data_function import (
        DataFunctionCallable,
        DataFunctionLike,
        DataFunction,
        ensure_datafunction,
    )
    from basis.core.external import ConfiguredExternalResource, ExternalResource
    from basis.core.function_node import FunctionNode, function_node_factory
    from basis.core.data_function_interface import FunctionGraphResolver
    from basis.core.runnable import ExecutionContext
    from basis.core.data_block import DataBlock

logger = logging.getLogger(__name__)


class Environment:
    library: ComponentLibrary
    storages: List[Storage]
    metadata_storage: Storage

    def __init__(
        self,
        name: str = None,
        metadata_storage: Union["Storage", str] = None,
        create_metadata_storage: bool = True,
        add_default_python_runtime: bool = True,
        initial_modules: List[BasisModule] = None,  # Defaults to `core` module
    ):
        from basis.core.runtime import Runtime
        from basis.core.runtime import RuntimeClass
        from basis.core.runtime import RuntimeEngine
        from basis.core.storage.storage import Storage
        from basis.modules import core

        self.name = name
        if metadata_storage is None and create_metadata_storage:
            # TODO: kind of hidden. make configurable at least, and log/print to user
            metadata_storage = Storage.from_url("sqlite:///.basis_metadata.db")
        if isinstance(metadata_storage, str):
            metadata_storage = Storage.from_url(metadata_storage)
        if metadata_storage is None:
            raise Exception("Must specify metadata_storage or allow default")
        self.metadata_storage = metadata_storage
        self.initialize_metadata_database()
        self._local_module = DEFAULT_LOCAL_MODULE  #     BasisModule(name=f"_env")
        self.library = ComponentLibrary(default_module=self._local_module)
        self._added_nodes: Dict[str, FunctionNode] = {}
        self._flattened_nodes: Dict[str, FunctionNode] = {}
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

    def initialize_metadata_database(self):
        from basis.core.metadata.listeners import add_persisting_sdb_listener
        from basis.core.storage.storage import StorageClass

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

    def close_sessions(self):
        for s in self._metadata_sessions:
            s.close()
        self._metadata_sessions = []

    def get_local_module(self) -> BasisModule:
        return self._local_module

    def get_module_order(self) -> List[str]:
        return self.library.module_precedence

    def get_otype(self, otype_like: ObjectTypeLike) -> ObjectType:
        if isinstance(otype_like, ObjectType):
            return otype_like
        try:
            return self.library.get_otype(otype_like)
        except KeyError:
            otype = self.get_generated_otype(otype_like)
            if otype is None:
                raise KeyError(otype_like)

    def get_generated_otype(self, otype_like: ObjectTypeLike) -> Optional[ObjectType]:
        c = ensure_uri(otype_like)
        if c.module_name:
            if not c.module_name == DEFAULT_LOCAL_MODULE.name:
                return None
        with self.session_scope() as sess:
            got = sess.query(GeneratedObjectType).get(c.name)
            if got is None:
                return None
            return got.as_otype()

    def add_new_otype(self, otype: ObjectType):
        if self.library.get_component(otype):
            # Already exists
            return
        got = GeneratedObjectType(name=otype.name, definition=asdict(otype))
        with self.session_scope() as sess:
            sess.add(got)
        self.library.add_component(otype)

    def get_function(self, df_like: Union[DataFunctionLike, str]) -> DataFunction:
        return self.library.get_function(df_like)

    def get_external_resource(
        self, ext_like: Union[ExternalResource, str]
    ) -> ExternalResource:
        return self.library.get_external_resource(ext_like)

    def add_node(
        self, name: str, function: Union[DataFunctionLike, str], **kwargs: Any
    ) -> FunctionNode:
        from basis.core.function_node import function_node_factory

        if isinstance(function, str):
            function = self.get_function(function)
        node = function_node_factory(self, name, function, **kwargs)
        self._added_nodes[node.name] = node
        self.register_node(node)
        return node

    def register_node(self, node: "FunctionNode"):
        if node.is_composite():
            for sub_node in node.get_nodes():
                self.register_node(sub_node)
        else:
            self._flattened_nodes[node.name] = node

    def add_external_source_node(
        self,
        name: str,
        external_resource: Union[ExternalResource, str],
        config: Dict,
        **kwargs: Any,
    ) -> FunctionNode:
        from basis.core.function_node import function_node_factory
        from basis.core.external import ConfiguredExternalResource

        if isinstance(external_resource, str):
            external_resource = self.library.get_external_resource(external_resource)
        p = external_resource.provider(name=name + "_provider", **config)
        r = external_resource(name=name + "_resource", **config, configured_provider=p)
        return self.add_node(name, r.extractor, **kwargs)

    def all_added_nodes(self) -> List[FunctionNode]:
        return list(self._added_nodes.values())

    def all_flattened_nodes(self) -> List[FunctionNode]:
        return list(self._flattened_nodes.values())

    def get_node(self, node_like: Union["FunctionNode", str]) -> "FunctionNode":
        from basis.core.function_node import FunctionNode

        if isinstance(node_like, FunctionNode):
            return node_like
        try:
            return self._added_nodes[node_like]
        except KeyError:  # TODO: do we want to get flattened (sub) nodes too? Probably
            return self._flattened_nodes[node_like]

    def get_function_graph_resolver(self) -> FunctionGraphResolver:
        from basis.core.data_function_interface import FunctionGraphResolver

        return FunctionGraphResolver(self)  # TODO: maybe cache this?

    def set_upstream(
        self, node_like: Union[FunctionNode, str], upstream: FunctionNodeRawInput
    ):
        n = self.get_node(node_like)
        n.set_upstream(upstream)

    def add_upstream(
        self, node_like: Union[FunctionNode, str], upstream: FunctionNodeRawInput
    ):
        n = self.get_node(node_like)
        n.add_upstream(upstream)

    def add_module(self, module: BasisModule):
        self.library.add_module(module)

    # def get_indexable_components(self) -> Iterable[IndexableComponent]:
    #     for module in self.module_registry.all():
    #         for c in module.get_indexable_components():
    #             yield c

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

    def get_execution_context(
        self, session: Session, target_storage: Storage = None, **kwargs
    ) -> ExecutionContext:
        from basis.core.runnable import ExecutionContext
        from basis.core.storage.storage import new_local_memory_storage

        if target_storage is None:
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
    def execution(self, target_storage: Storage = None, **kwargs):
        # TODO: target storage??
        from basis.core.runnable import ExecutionManager

        session = self.Session()
        ec = self.get_execution_context(
            session, target_storage=target_storage, **kwargs
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
        node_like: Union[FunctionNode, str],
        to_exhaustion: bool = True,
        **execution_kwargs: Any,
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
                output = em.run(dep, to_exhaustion=to_exhaustion)
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
        from basis.core.storage.storage import Storage

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
                print(f"#{block.id} {block.declared_otype_uri} is orphaned! SAD")
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
#     from basis.core.storage.storage import StorageResource
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
    from basis.core.storage.storage import Storage

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
