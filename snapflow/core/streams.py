from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Union,
)

from loguru import logger
from snapflow.core.data_block import (
    DataBlock,
    DataBlockMetadata,
    StoredDataBlockMetadata,
)
from snapflow.core.environment import Environment
from snapflow.core.graph import Graph
from snapflow.core.node import (
    DataBlockLog,
    DeclaredNode,
    Direction,
    Node,
    NodeLike,
    PipeLog,
)
from snapflow.core.pipe_interface import get_schema_translation
from snapflow.core.storage.storage import Storage
from snapflow.core.typing.schema import Schema, SchemaLike, SchemaTranslation
from snapflow.utils.common import ensure_list
from sqlalchemy import and_, not_
from sqlalchemy.orm import Query

if TYPE_CHECKING:
    from snapflow.core.runnable import ExecutionContext
    from snapflow.core.operators import Operator, BoundOperator


class StreamBuilder:
    """"""

    def __init__(
        self,
        nodes: Union[NodeLike, List[NodeLike]] = None,
        schemas: List[SchemaLike] = None,
        storages: List[Storage] = None,
        schema: SchemaLike = None,
        storage: Storage = None,
        unprocessed_by: Node = None,
        data_block: Union[DataBlockMetadata, DataBlock, str] = None,
        allow_cycle: bool = False,
        most_recent_first: bool = False,
        operators: List[BoundOperator] = None,
    ):
        # TODO: ugly duplicate params (but singulars give nice obvious/intuitive interface)
        if schema is not None:
            assert schemas is None
            schemas = [schema]
        if storage is not None:
            assert storages is None
            storages = [storage]
        # TODO: make all these private?
        self.nodes = nodes
        self.schemas = schemas
        self.storages = storages
        self.data_block = data_block
        self.unprocessed_by = unprocessed_by
        self.allow_cycle = allow_cycle
        self.most_recent_first = most_recent_first
        self.operators = operators

    def __str__(self):
        s = "Stream("
        if self.nodes:
            inputs = [
                i if isinstance(i, str) else i.key for i in ensure_list(self.nodes)
            ]
            s += f"inputs={inputs}"
        s += ")"
        return s

    def _base_query(self) -> Query:
        return Query(DataBlockMetadata).order_by(DataBlockMetadata.id)

    def get_query(self, ctx: ExecutionContext) -> Query:
        q = self._base_query()
        if self.nodes is not None:
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

    def clone(self, **kwargs) -> StreamBuilder:
        args = dict(
            nodes=self.nodes,
            schemas=self.schemas,
            storages=self.storages,
            unprocessed_by=self.unprocessed_by,
            allow_cycle=self.allow_cycle,
            most_recent_first=self.most_recent_first,
            operators=self.operators,
        )
        args.update(**kwargs)
        return StreamBuilder(**args)  # type: ignore

    def source_node_keys(self) -> List[str]:
        keys: List[str] = []
        for n in ensure_list(self.nodes):
            if isinstance(n, str):
                keys.append(n)
            else:
                keys.append(n.key)
        return keys

    def filter_unprocessed(
        self, unprocessed_by: Node, allow_cycle=False
    ) -> StreamBuilder:
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
        nodes = ensure_list(self.nodes)
        if not nodes:
            return []
        return [g.get_node(c) for c in nodes]

    def filter_upstream(self, upstream: Union[Node, List[Node]]) -> StreamBuilder:
        return self.clone(
            upstream=ensure_list(upstream),
        )

    def _filter_upstream(
        self,
        ctx: ExecutionContext,
        query: Query,
    ) -> Query:
        if not self.nodes:
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

    def filter_schemas(self, schemas: List[SchemaLike]) -> StreamBuilder:
        return self.clone(schemas=schemas)

    def _filter_schemas(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.schemas:
            return query
        return query.filter(
            DataBlockMetadata.nominal_schema_key.in_([d.key for d in self.get_schemas(ctx.env)])  # type: ignore
        )

    def filter_schema(self, schema: SchemaLike) -> StreamBuilder:
        return self.filter_schemas(ensure_list(schema))

    def filter_storages(self, storages: List[Storage]) -> StreamBuilder:
        return self.clone(storages=storages)

    def _filter_storages(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.storages:
            return query
        return query.join(StoredDataBlockMetadata).filter(
            StoredDataBlockMetadata.storage_url.in_([s.url for s in self.storages])  # type: ignore
        )

    def filter_storage(self, storage: Storage) -> StreamBuilder:
        return self.filter_storages(ensure_list(storage))

    def filter_data_block(
        self, data_block: Union[DataBlockMetadata, DataBlock, str]
    ) -> StreamBuilder:
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

    def get_operators(self) -> List[BoundOperator]:
        return self.operators or []

    def apply_operator(self, op: BoundOperator) -> StreamBuilder:
        return self.clone(operators=(self.get_operators() + [op]))

    def is_unprocessed(
        self,
        ctx: ExecutionContext,
        block: DataBlockMetadata,
        node: Node,
    ) -> bool:
        blocks = self.filter_unprocessed(node)
        q = blocks.get_query(ctx)
        return q.filter(DataBlockMetadata.id == block.id).count() > 0

    def get_count(self, ctx: ExecutionContext) -> int:
        return self.get_query(ctx).count()

    def get_all(self, ctx: ExecutionContext) -> List[DataBlockMetadata]:
        return self.get_query(ctx).all()

    def as_managed_stream(
        self,
        ctx: ExecutionContext,
        declared_schema: Optional[Schema] = None,
        declared_schema_translation: Optional[Dict[str, str]] = None,
    ) -> ManagedDataBlockStream:
        return ManagedDataBlockStream(
            ctx,
            self,
            declared_schema=declared_schema,
            declared_schema_translation=declared_schema_translation,
        )


def block_as_stream_builder(data_block: DataBlockMetadata) -> StreamBuilder:
    return StreamBuilder(data_block=data_block)


def block_as_stream(
    data_block: DataBlockMetadata,
    ctx: ExecutionContext,
    declared_schema: Optional[Schema] = None,
    declared_schema_translation: Optional[Dict[str, str]] = None,
) -> DataBlockStream:
    stream = block_as_stream_builder(data_block)
    return stream.as_managed_stream(ctx, declared_schema, declared_schema_translation)


class ManagedDataBlockStream:
    def __init__(
        self,
        ctx: ExecutionContext,
        stream_builder: StreamBuilder,
        declared_schema: Optional[Schema] = None,
        declared_schema_translation: Optional[Dict[str, str]] = None,
    ):
        self.ctx = ctx
        self.declared_schema = declared_schema
        self.declared_schema_translation = declared_schema_translation
        self._blocks: List[DataBlock] = list(self._build_stream(stream_builder))
        self._stream: Iterator[DataBlock] = self.log_emitted(self._blocks)
        self._emitted_blocks: List[DataBlockMetadata] = []
        self._emitted_managed_blocks: List[DataBlock] = []

    def _build_stream(self, stream_builder: StreamBuilder) -> Iterator[DataBlock]:
        query = stream_builder.get_query(self.ctx)
        stream = (b for b in query)
        stream = self.as_managed_block(stream)
        for op in stream_builder.get_operators():
            stream = op.op_callable(stream, **op.kwargs)
        return stream

    def __iter__(self) -> Iterator[DataBlock]:
        return self._stream

    def __next__(self) -> DataBlock:
        return next(self._stream)

    def as_managed_block(
        self, stream: Iterator[DataBlockMetadata]
    ) -> Iterator[DataBlock]:
        for db in stream:
            schema_translation = get_schema_translation(
                self.ctx.env,
                db,
                declared_schema=self.declared_schema,
                declared_schema_translation=self.declared_schema_translation,
            )
            mdb = db.as_managed_data_block(
                self.ctx, schema_translation=schema_translation
            )
            yield mdb

    @property
    def all_blocks(self) -> List[DataBlock]:
        return self._blocks

    def count(self) -> int:
        return len(self._blocks)

    def log_emitted(self, stream: Iterator[DataBlock]) -> Iterator[DataBlock]:
        for mdb in stream:
            self._emitted_blocks.append(mdb.data_block_metadata)
            self._emitted_managed_blocks.append(mdb)
            yield mdb

    def get_emitted_blocks(self) -> List[DataBlockMetadata]:
        return self._emitted_blocks

    def get_emitted_managed_blocks(self) -> List[DataBlock]:
        return self._emitted_managed_blocks


DataBlockStream = Iterator[DataBlock]
Stream = DataBlockStream

StreamLike = Union[StreamBuilder, NodeLike]
DataBlockStreamable = Union[StreamBuilder, Node]
InputStreams = Dict[str, DataBlockStream]
InputBlocks = Dict[str, DataBlockMetadata]
