from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

from sqlalchemy import and_, not_
from sqlalchemy.orm import Query

from dags.core.data_block import DataBlock, DataBlockMetadata, StoredDataBlockMetadata
from dags.core.environment import Environment
from dags.core.graph import Graph
from dags.core.node import DataBlockLog, Direction, Node, PipeLog
from dags.core.storage.storage import Storage
from dags.core.typing.object_schema import ObjectSchema, ObjectSchemaLike
from dags.utils.common import ensure_list
from loguru import logger

if TYPE_CHECKING:
    from dags.core.runnable import ExecutionContext


class DataBlockStream:
    """"""

    def __init__(
        self,
        upstream: Union[Node, List[Node]] = None,
        schemas: List[ObjectSchemaLike] = None,
        storages: List[Storage] = None,
        schema: ObjectSchemaLike = None,
        storage: Storage = None,
        unprocessed_by: Node = None,
        data_sets: List[str] = None,
        data_sets_only: bool = False,
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
        self.data_sets = data_sets
        self.data_sets_only = data_sets_only
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
        if self.data_sets:
            s += " datasets={self.datasets}"
        s += ")"
        return s

    def _base_query(self) -> Query:
        return Query(DataBlockMetadata)

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
        if self.data_sets is not None:
            q = self._filter_datasets(ctx, q)
        if self.data_block is not None:
            q = self._filter_data_block(ctx, q)
        return q.with_session(ctx.metadata_session)

    def clone(self, **kwargs) -> DataBlockStream:
        args = dict(
            upstream=self.upstream,
            schemas=self.schemas,
            storages=self.storages,
            unprocessed_by=self.unprocessed_by,
            data_sets=self.data_sets,
            data_sets_only=self.data_sets_only,
            allow_cycle=self.allow_cycle,
            most_recent_first=self.most_recent_first,
        )
        args.update(**kwargs)
        return DataBlockStream(**args)  # type: ignore

    def filter_unprocessed(
        self, unprocessed_by: Node, allow_cycle=False
    ) -> DataBlockStream:
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

    def filter_upstream(self, upstream: Union[Node, List[Node]]) -> DataBlockStream:
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

    def filter_schemas(self, schemas: List[ObjectSchemaLike]) -> DataBlockStream:
        return self.clone(schemas=schemas)

    def _filter_schemas(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.schemas:
            return query
        return query.filter(
            DataBlockMetadata.expected_schema_key.in_([d.key for d in self.get_schemas(ctx.env)])  # type: ignore
        )

    def filter_schema(self, schema: ObjectSchemaLike) -> DataBlockStream:
        return self.filter_schemas(ensure_list(schema))

    def filter_storages(self, storages: List[Storage]) -> DataBlockStream:
        return self.clone(storages=storages)

    def _filter_storages(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.storages:
            return query
        return query.join(StoredDataBlockMetadata).filter(
            StoredDataBlockMetadata.storage_url.in_([s.url for s in self.storages])  # type: ignore
        )

    def filter_storage(self, storage: Storage) -> DataBlockStream:
        return self.filter_storages(ensure_list(storage))

    def filter_data_block(
        self, data_block: Union[DataBlockMetadata, DataBlock, str]
    ) -> DataBlockStream:
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

    def get_next(self, ctx: ExecutionContext) -> Optional[DataBlockMetadata]:
        order_by = DataBlockMetadata.updated_at
        if self.most_recent_first:
            order_by = order_by.desc()
        return (
            self.get_query(ctx).order_by(order_by).first()
        )  # Ordered by creation id (order created in) (since blocks are immutable)

    def get_most_recent(self, ctx: ExecutionContext) -> Optional[DataBlockMetadata]:
        return (
            # self.get_query(ctx).order_by(DataBlockMetadata.updated_at.desc()).first()
            self.get_query(ctx)
            .order_by(DataBlockMetadata.id.desc())
            .first()  # We use auto-inc ID instead of timestamp since timestamps can collide
        )

    def get_count(self, ctx: ExecutionContext) -> int:
        return self.get_query(ctx).count()

    def get_all(self, ctx: ExecutionContext) -> List[DataBlockMetadata]:
        return self.get_query(ctx).all()


DataBlockStreamable = Union[DataBlockStream, Node]
DataBlockStreamLike = Union[DataBlockStreamable, str]
PipeNodeRawInput = Any  # Union[DataBlockStreamLike, Dict[str, DataBlockStreamLike]] + records object formats # TODO
PipeNodeInput = Union[DataBlockStreamable, Dict[str, DataBlockStreamable]]
InputStreams = Union[DataBlockStream, Dict[str, DataBlockStream]]
InputBlocks = Dict[str, DataBlockMetadata]


def ensure_data_stream(s: DataBlockStreamable) -> DataBlockStream:
    return s if isinstance(s, DataBlockStream) else s.as_stream()
