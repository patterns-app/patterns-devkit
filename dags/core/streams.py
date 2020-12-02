from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Set, Union

from sqlalchemy import and_, not_
from sqlalchemy.orm import Query

from dags.core.data_block import DataBlock, DataBlockMetadata, StoredDataBlockMetadata
from dags.core.environment import Environment
from dags.core.graph import Graph
from dags.core.node import DataBlockLog, Direction, Node, PipeLog
from dags.core.pipe_interface import get_schema_mapping
from dags.core.storage.storage import Storage
from dags.core.typing.object_schema import ObjectSchema, ObjectSchemaLike, SchemaMapping
from dags.utils.common import ensure_list
from loguru import logger

if TYPE_CHECKING:
    from dags.core.runnable import ExecutionContext


class DataBlockStreamBuilder:
    """"""

    def __init__(
        self,
        upstream: Union[Node, List[Node]] = None,
        schemas: List[ObjectSchemaLike] = None,
        storages: List[Storage] = None,
        schema: ObjectSchemaLike = None,
        storage: Storage = None,
        unprocessed_by: Node = None,
        data_block: Union[DataBlockMetadata, DataBlock, str] = None,
        allow_cycle: bool = False,
        most_recent_first: bool = False,
    ):
        # TODO: ugly duplicate params (but singulars give nice obvious/intuitive interface)
        if schema is not None:
            assert schemas is None
            schemas = [schema]
        if storage is not None:
            assert storages is None
            storages = [storage]
        # TODO: make all these private?
        self.upstream = upstream
        self.schemas = schemas
        self.storages = storages
        self.data_block = data_block
        self.unprocessed_by = unprocessed_by
        self.allow_cycle = allow_cycle
        self.most_recent_first = most_recent_first

    def __str__(self):
        s = "Stream("
        if self.upstream:
            inputs = [
                i if isinstance(i, str) else i.key for i in ensure_list(self.upstream)
            ]
            s += f"inputs={inputs}"
        s += ")"
        return s

    def _base_query(self) -> Query:
        return Query(DataBlockMetadata).order_by(DataBlockMetadata.id)

    def get_query(self, ctx: ExecutionContext) -> Query:
        q = self._base_query()
        if self.upstream is not None:
            q = self._filter_upstream(ctx, q)
        if self.schemas is not None:
            q = self._filter_schemas(ctx, q)
        if self.storages is not None:
            q = self._filter_storages(ctx, q)
        if self.unprocessed_by is not None:
            q = self._filter_unprocessed(ctx, q)
        if self.data_block is not None:
            q = self._filter_data_block(ctx, q)
        return q.with_session(ctx.metadata_session)

    def clone(self, **kwargs) -> DataBlockStreamBuilder:
        args = dict(
            upstream=self.upstream,
            schemas=self.schemas,
            storages=self.storages,
            unprocessed_by=self.unprocessed_by,
            allow_cycle=self.allow_cycle,
            most_recent_first=self.most_recent_first,
        )
        args.update(**kwargs)
        return DataBlockStreamBuilder(**args)  # type: ignore

    def filter_unprocessed(
        self, unprocessed_by: Node, allow_cycle=False
    ) -> DataBlockStreamBuilder:
        return self.clone(
            unprocessed_by=unprocessed_by,
            allow_cycle=allow_cycle,
        )

    def _filter_unprocessed(
        self,
        ctx: ExecutionContext,
        query: Query,
    ) -> Query:
        if not self.unprocessed_by:
            return query
        if self.allow_cycle:
            # Only exclude blocks processed as INPUT
            filter_clause = and_(
                DataBlockLog.direction == Direction.INPUT,
                PipeLog.node_key == self.unprocessed_by.key,
            )
        else:
            # No block cycles allowed
            # Exclude blocks processed as INPUT and blocks outputted
            filter_clause = PipeLog.node_key == self.unprocessed_by.key
        already_processed_drs = (
            Query(DataBlockLog.data_block_id)
            .join(PipeLog)
            .filter(filter_clause)
            .distinct()
        )
        return query.filter(not_(DataBlockMetadata.id.in_(already_processed_drs)))

    def get_upstream(self, g: Graph) -> List[Node]:
        nodes = ensure_list(self.upstream)
        if not nodes:
            return []
        return [g.get_node(c) for c in nodes]

    def filter_upstream(
        self, upstream: Union[Node, List[Node]]
    ) -> DataBlockStreamBuilder:
        return self.clone(
            upstream=ensure_list(upstream),
        )

    def _filter_upstream(
        self,
        ctx: ExecutionContext,
        query: Query,
    ) -> Query:
        if not self.upstream:
            return query
        eligible_input_drs = (
            Query(DataBlockLog.data_block_id)
            .join(PipeLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                PipeLog.node_key.in_([c.key for c in self.get_upstream(ctx.graph)]),
            )
            .distinct()
        )
        return query.filter(DataBlockMetadata.id.in_(eligible_input_drs))

    def get_schemas(self, env: Environment):
        dts = ensure_list(self.schemas)
        return [env.get_schema(d) for d in dts]

    def filter_schemas(self, schemas: List[ObjectSchemaLike]) -> DataBlockStreamBuilder:
        return self.clone(schemas=schemas)

    def _filter_schemas(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.schemas:
            return query
        return query.filter(
            DataBlockMetadata.expected_schema_key.in_([d.key for d in self.get_schemas(ctx.env)])  # type: ignore
        )

    def filter_schema(self, schema: ObjectSchemaLike) -> DataBlockStreamBuilder:
        return self.filter_schemas(ensure_list(schema))

    def filter_storages(self, storages: List[Storage]) -> DataBlockStreamBuilder:
        return self.clone(storages=storages)

    def _filter_storages(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.storages:
            return query
        return query.join(StoredDataBlockMetadata).filter(
            StoredDataBlockMetadata.storage_url.in_([s.url for s in self.storages])  # type: ignore
        )

    def filter_storage(self, storage: Storage) -> DataBlockStreamBuilder:
        return self.filter_storages(ensure_list(storage))

    def filter_data_block(
        self, data_block: Union[DataBlockMetadata, DataBlock, str]
    ) -> DataBlockStreamBuilder:
        return self.clone(data_block=data_block)

    def _filter_data_block(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.data_block:
            return query
        if isinstance(self.data_block, str):
            db_id = self.data_block
        elif isinstance(self.data_block, DataBlockMetadata):
            db_id = self.data_block.id
        elif isinstance(self.data_block, DataBlock):
            db_id = self.data_block.data_block_id
        else:
            raise TypeError(self.data_block)
        return query.filter(DataBlockMetadata.id == db_id)

    def is_unprocessed(
        self,
        ctx: ExecutionContext,
        block: DataBlockMetadata,
        node: Node,
    ) -> bool:
        blocks = self.filter_unprocessed(node)
        q = blocks.get_query(ctx)
        return q.filter(DataBlockMetadata.id == block.id).count() > 0

    # def get_next(self, ctx: ExecutionContext) -> Optional[DataBlockMetadata]:
    #     order_by = DataBlockMetadata.updated_at
    #     # if self.most_recent_first:
    #     #     order_by = order_by.desc()
    #     return self.get_query(
    #         ctx
    #     ).first()  # Ordered by creation id (order created in) (since blocks are immutable)

    # def get_most_recent(self, ctx: ExecutionContext) -> Optional[DataBlockMetadata]:
    #     return (
    #         # self.get_query(ctx).order_by(DataBlockMetadata.updated_at.desc()).first()
    #         self.get_query(ctx)
    #         .order_by(DataBlockMetadata.id.desc())
    #         .first()  # We use auto-inc ID instead of timestamp since timestamps can collide
    #     )

    def get_count(self, ctx: ExecutionContext) -> int:
        return self.get_query(ctx).count()

    def get_all(self, ctx: ExecutionContext) -> List[DataBlockMetadata]:
        return self.get_query(ctx).all()

    def as_managed_stream(
        self,
        ctx: ExecutionContext,
        expected_schema: Optional[ObjectSchema] = None,
        declared_schema_mapping: Optional[SchemaMapping] = None,
    ) -> ManagedDataBlockStream:
        return ManagedDataBlockStream(
            ctx,
            self,
            expected_schema=expected_schema,
            declared_schema_mapping=declared_schema_mapping,
        )


class ManagedDataBlockStream:
    def __init__(
        self,
        ctx: ExecutionContext,
        stream: DataBlockStreamBuilder,
        expected_schema: Optional[ObjectSchema] = None,
        declared_schema_mapping: Optional[Dict[str, str]] = None,
    ):
        self.ctx = ctx
        self.stream = stream
        self.expected_schema = expected_schema
        self.declared_schema_mapping = declared_schema_mapping
        self._blocks = self.stream.get_query(self.ctx)
        self._emitted_blocks: List[DataBlockMetadata] = []
        self._emitted_managed_blocks: List[DataBlock] = []

    def next(self) -> Optional[DataBlock]:
        db = next(self._blocks)
        self._emitted_blocks.append(db)
        schema_mapping = get_schema_mapping(
            self.ctx.env,
            db,
            expected_schema=self.expected_schema,
            declared_schema_mapping=self.declared_schema_mapping,
        )
        mdb = db.as_managed_data_block(self.ctx, schema_mapping=schema_mapping)
        self._emitted_managed_blocks.append(mdb)
        return mdb

    def get_emitted_blocks(self) -> List[DataBlockMetadata]:
        return self._emitted_blocks

    def get_emitted_managed_blocks(self) -> List[DataBlock]:
        return self._emitted_managed_blocks

    def count(self) -> int:
        # Non-consuming
        return self._blocks.count()


DataBlockStream = ManagedDataBlockStream

DataBlockStreamable = Union[DataBlockStreamBuilder, Node]
InputStreams = Dict[str, DataBlockStream]
InputBlocks = Dict[str, DataBlockMetadata]


def ensure_data_stream_builder(s: DataBlockStreamable) -> DataBlockStreamBuilder:
    return s if isinstance(s, DataBlockStreamBuilder) else s.as_stream()
