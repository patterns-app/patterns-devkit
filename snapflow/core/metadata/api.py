from __future__ import annotations

import pathlib
from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Union

from alembic import command
from alembic.config import Config
from dcp import Storage
from dcp.storage.base import DatabaseStorageClass
from sqlalchemy.engine import Result
from sqlalchemy.engine.base import Connection
from sqlalchemy.orm.session import Session, SessionTransaction, sessionmaker
from sqlalchemy.sql import Delete, Update
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.selectable import Select


class MetadataApi:
    def __init__(self, env_id: str, storage: Storage, initialize: bool = False):
        self.env_id = env_id
        self.storage = storage
        self.engine = self.storage.get_api().get_engine()
        self.Session = sessionmaker(self.engine)
        if initialize:
            self.initialize_metadata_database()
        self.active_session = None

    @contextmanager
    def begin(self) -> Iterator[Session]:
        if self.active_session is None:
            with self.Session.begin() as sess:
                self.active_session = sess
                yield sess
            self.active_session = None
        else:
            # TODO: handle nested tx
            yield self.active_session

    @contextmanager
    def begin_nested(self) -> Iterator[SessionTransaction]:
        assert self.active_session is not None
        with self.active_session.begin_nested() as sess_tx:
            yield sess_tx

    @contextmanager
    def ensure_session(self) -> Iterator[Session]:
        if self.active_session is None:
            with self.Session.begin() as sess:
                self.active_session = sess
                yield sess
            self.active_session = None
        else:
            yield self.active_session

    def get_session(self) -> Session:
        if self.active_session is None:
            raise ValueError(
                "No metadata session active. Call MetadataApi.begin() beforehand"
            )
        return self.active_session

    def augment_statement(
        self, stmt: Union[Select, Update, Delete], filter_env: bool = True
    ) -> Select:
        if filter_env:
            stmt = stmt.filter_by(env_id=self.env_id)
        return stmt

    def execute(
        self, stmt: Union[Select, Update, Delete], filter_env: bool = True
    ) -> Result:
        stmt = self.augment_statement(stmt, filter_env=filter_env)
        return self.get_session().execute(stmt)

    def count(self, stmt: Select, filter_env: bool = True) -> int:
        stmt = select(func.count()).select_from(stmt.subquery())
        return self.execute(stmt).scalar_one()

    def add(self, obj: Any, set_env: bool = True):
        if obj.env_id is None and set_env:
            obj.env_id = self.env_id
        self.get_session().add(obj)

    def add_all(self, objects: Iterable, set_env: bool = True):
        for obj in objects:
            if obj.env_id is None and set_env:
                obj.env_id = self.env_id
        self.get_session().add_all(objects)

    def flush(self, objects=None):
        if objects:
            self.get_session().flush(objects)
        else:
            self.get_session().flush()

    def delete(self, obj):
        sess = self.get_session()
        if obj in sess.new:
            sess.expunge(obj)
        else:
            sess.delete(obj)

    def commit(self):
        self.get_session().commit()

    ### Alembic

    def initialize_metadata_database(self):
        if not issubclass(
            self.storage.storage_engine.storage_class, DatabaseStorageClass
        ):
            raise ValueError(
                f"metadata storage expected a database, got {self.storage}"
            )
        # BaseModel.metadata.create_all(conn)
        # try:
        self.migrate_metdata_database()
        # except SQLAlchemyError as e:
        #     # Catch database exception, meaning already created, just stamp
        #     # For initial migration
        #     # TODO: remove once all 0.2 systems migrated?
        #     logger.warning(e)
        #     self.stamp_metadata_database()
        #     self.migrate_metdata_database()

    def migrate_metdata_database(self):
        alembic_cfg = self.get_alembic_config()
        if self.engine is not None:
            alembic_cfg.attributes["connection"] = self.engine
        command.upgrade(alembic_cfg, "head")

    def get_alembic_config(self) -> Config:
        dir_path = pathlib.Path(__file__).parent.absolute()
        cfg_path = dir_path / "../../migrations/alembic.ini"
        alembic_cfg = Config(str(cfg_path))
        alembic_cfg.set_main_option("sqlalchemy.url", self.storage.url)
        return alembic_cfg

    def stamp_metadata_database(self):
        alembic_cfg = self.get_alembic_config()
        if self.engine is not None:
            alembic_cfg.attributes["connection"] = self.engine
        command.stamp(alembic_cfg, "23dd1cc88eb2")
