from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from importlib import import_module
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Union

from sqlalchemy.orm import Session, sessionmaker

from basis.core.metadata.orm import BaseModel
from basis.core.module import BasisModule
from basis.core.object_type import DEFAULT_MODULE_KEY, ObjectType, ObjectTypeLike
from basis.utils.registry import Registry, UriRegistry

if TYPE_CHECKING:
    from basis.core.storage_resource import (
        StorageResource,
        new_local_memory_storage,
    )
    from basis.core.data_function import (
        ConfiguredDataFunction,
        DataFunctionCallable,
        configured_data_function_factory,
    )
    from basis.indexing.components import IndexableComponent
    from basis.core.runnable import ExecutionContext
    from basis.core.data_resource import DataResource

logger = logging.getLogger(__name__)


class Environment:
    otype_registry: UriRegistry
    storages: List[StorageResource]
    metadata_storage_resource: StorageResource

    def __init__(
        self,
        key: str = None,
        metadata_storage_resource: "StorageResource" = None,
        configured_data_function_registry: Registry = None,
        otype_registry: UriRegistry = None,
        source_registry: Registry = None,
        # TODO: SourceResource registry too?
        module_registry: Registry = None,
        create_metadata_storage: bool = True,
        add_default_python_runtime: bool = True,
    ):
        from basis.core.runtime_resource import RuntimeResource
        from basis.core.runtime_resource import RuntimeClass
        from basis.core.runtime_resource import RuntimeEngine
        from basis.core.storage_resource import StorageResource

        self.key = key

        if metadata_storage_resource is None and create_metadata_storage:

            # TODO: kind of hidden. make configurable at least, and log/print to user
            metadata_storage_resource = StorageResource.from_url(
                "sqlite:///.basis_metadata.db"
            )
        if isinstance(metadata_storage_resource, str):
            metadata_storage_resource = StorageResource.from_url(
                metadata_storage_resource
            )
        if metadata_storage_resource is None:
            raise Exception("Must specify metadata_storage_resource or allow default")
        self.metadata_storage_resource = metadata_storage_resource
        # if create_metadata_storage: # TODO: hmmm
        self.initialize_metadata_database()
        self.module_registry = module_registry or Registry()
        self.otype_registry = otype_registry or UriRegistry()
        self.source_registry = source_registry or Registry()
        self.configured_data_function_registry = (
            configured_data_function_registry or Registry()
        )
        self.storages = []
        self.runtimes = []
        if add_default_python_runtime:
            self.runtimes.append(
                RuntimeResource(
                    url="python://local",
                    runtime_class=RuntimeClass.PYTHON,
                    runtime_engine=RuntimeEngine.LOCAL,
                )
            )
        self._module_order: List[str] = []

    def initialize_metadata_database(self):
        from basis.core.metadata.listeners import add_persisting_sdr_listener

        conn = self.metadata_storage_resource.get_database_api(self).get_connection()
        BaseModel.metadata.create_all(conn)
        self.Session = sessionmaker(bind=conn)
        add_persisting_sdr_listener(self.Session)
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
        return self.otype_registry.get(otype_like, module_order=self.get_module_order())

    def node(
        self, _key: str, _data_function: DataFunctionCallable, **kwargs
    ) -> ConfiguredDataFunction:
        from basis.core.data_function import configured_data_function_factory

        cdf = configured_data_function_factory(self, _key, _data_function, **kwargs)
        self.configured_data_function_registry.register(cdf)
        return cdf

    # def get_source(self, source_like: ObjectTypeLike) -> ObjectType:
    #     if isinstance(otype_like, ObjectType):
    #         return otype_like
    #     return self.otype_registry.get(otype_like)

    def register_node(self, cdf: "ConfiguredDataFunction"):
        self.configured_data_function_registry.register(cdf)

    def get_node(
        self, cdf_like: Union["ConfiguredDataFunction", str]
    ) -> "ConfiguredDataFunction":
        from basis.core.data_function import ConfiguredDataFunction

        if isinstance(cdf_like, ConfiguredDataFunction):
            return cdf_like
        return self.configured_data_function_registry.get(cdf_like)

    def add_module(self, module: BasisModule):
        self.module_registry.register(module)
        self.otype_registry.merge(module.otypes)
        self.source_registry.merge(module.sources)
        self._module_order.append(module.key)

    def get_indexable_components(self) -> Iterable[IndexableComponent]:
        for module in self.module_registry:
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
        from basis.core.storage_resource import new_local_memory_storage

        target_storage = self.storages[0] if self.storages else None
        args = dict(
            env=self,
            metadata_session=session,
            runtime_resources=self.runtimes,
            storage_resources=self.storages,
            target_storage=target_storage,
            local_memory_storage=new_local_memory_storage(),
        )
        args.update(**kwargs)
        return ExecutionContext(**args)

    @contextmanager
    def execution(self, target_storage: StorageResource = None):
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
            # self.validate_and_clean_data_resources(delete_intermediate=True)
            session.close()

    def produce(
        self, node_like: Union[ConfiguredDataFunction, str]
    ) -> Optional[DataResource]:
        from basis.core.data_function import ConfiguredDataFunction
        from basis.core.graph import get_all_upstream_dependencies_in_execution_order

        if isinstance(node_like, str):
            node_like = self.get_node(node_like)
        assert isinstance(node_like, ConfiguredDataFunction)
        dependencies = get_all_upstream_dependencies_in_execution_order(self, node_like)
        for dep in dependencies:
            with self.execution() as em:
                em.run(dep, to_exhaustion=True)

    def get_latest_output(self, cdf: ConfiguredDataFunction) -> Optional[DataResource]:
        session = self.get_new_metadata_session()  # TODO: hanging session
        ctx = self.get_execution_context(session)
        return cdf.get_latest_output(ctx)

    def add_storage(
        self, storage_like: Union[StorageResource, str], add_runtime=True
    ) -> StorageResource:
        from basis.core.storage_resource import StorageResource

        if isinstance(storage_like, str):
            sr = StorageResource.from_url(storage_like)
        elif isinstance(storage_like, StorageResource):
            sr = storage_like
        else:
            raise TypeError
        self.storages.append(sr)
        if add_runtime:
            from basis.core.runtime_resource import RuntimeResource

            try:
                rt = RuntimeResource.from_storage_resource(sr)
                self.runtimes.append(rt)
            except ValueError:
                pass
        return sr

    def validate_and_clean_data_resources(
        self, delete_memory=True, delete_intermediate=False, force: bool = False
    ):
        with self.session_scope() as sess:
            from basis.core.data_resource import (
                DataResourceMetadata,
                StoredDataResourceMetadata,
            )

            if delete_memory:
                deleted = (
                    sess.query(StoredDataResourceMetadata)
                    .filter(
                        StoredDataResourceMetadata.storage_resource_url.startswith(
                            "memory:"
                        )
                    )
                    .delete(False)
                )
                print(f"{deleted} Memory SDRs deleted")

            for dr in sess.query(DataResourceMetadata).filter(
                ~DataResourceMetadata.stored_data_resources.any()
            ):
                print(f"#{dr.id} {dr.otype_uri} is orphaned! SAD")
            if delete_intermediate:
                # TODO: does no checking if they are unprocessed or not...
                if not force:
                    d = input(
                        "Are you sure you want to delete ALL intermediate DataResources? There is no undoing this operation. y/N?"
                    )
                    if not d or d.lower()[0] != "y":
                        return
                # Delete DRs with no DataSet
                cnt = (
                    sess.query(DataResourceMetadata)
                    .filter(~DataResourceMetadata.data_sets.any(),)
                    .update(
                        {DataResourceMetadata.deleted: True}, synchronize_session=False
                    )
                )
                print(f"{cnt} intermediate DRs deleted")


# Not supporting yml project config atm
# def load_environment_from_yaml(yml) -> Environment:
#     from basis.core.storage_resource import StorageResource
#
#     env = Environment(
#         metadata_storage_resource=yml.get("metadata_storage", None),
#         add_default_python_runtime=yml.get("add_default_python_runtime", True),
#     )
#     for url in yml.get("storages"):
#         env.add_storage(StorageResource.from_url(url))
#     for module_name in yml.get("modules"):
#         m = import_module(module_name)
#         env.add_module(m)
#     return env


def load_environment_from_project(project: Any) -> Environment:
    from basis.core.storage_resource import StorageResource

    env = Environment(
        metadata_storage_resource=getattr(project, "metadata_storage", None),
        add_default_python_runtime=getattr(project, "add_default_python_runtime", True),
    )
    for url in getattr(project, "storages", []):
        env.add_storage(StorageResource.from_url(url))
    for module_name in getattr(project, "modules", []):
        m = import_module(module_name)
        env.add_module(m)  # type: ignore
    return env


def current_env(cfg_module: str = "project") -> Environment:
    import sys

    sys.path.append(os.getcwd())
    cfg = import_module(cfg_module)
    return load_environment_from_project(cfg)
