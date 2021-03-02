from __future__ import annotations

from dataclasses import asdict, dataclass, field
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
    SnapLog,
)
from snapflow.core.snap_interface import get_schema_translation
from snapflow.schema.base import Schema, SchemaLike, SchemaTranslation
from snapflow.storage.storage import Storage
from snapflow.utils.common import ensure_list
from sqlalchemy import and_, not_
from sqlalchemy.orm import Query
from sqlalchemy.orm.session import Session

if TYPE_CHECKING:
    from snapflow.core.execution import RunContext
    from snapflow.core.operators import Operator, BoundOperator


def ensure_data_block_id(
    data_block: Union[DataBlockMetadata, DataBlock, str, None]
) -> Optional[str]:
    if data_block is None:
        return None
    db_id = None
    if isinstance(data_block, str):
        db_id = data_block
    elif isinstance(data_block, DataBlockMetadata):
        db_id = data_block.id
    elif isinstance(data_block, DataBlock):
        db_id = data_block.data_block_id
    else:
        raise TypeError(data_block)
    return db_id


def ensure_schema_key(schema: SchemaLike) -> str:
    if isinstance(schema, Schema):
        return schema.key
    return schema


def ensure_node_key(node: NodeLike) -> str:
    if isinstance(node, Node) or isinstance(node, DeclaredNode):
        return node.key
    return node


@dataclass(frozen=True)
class StreamBuilderSerializable:
    node_keys: List[str] = field(default_factory=list)
    schema_keys: List[str] = field(default_factory=list)
    storage_urls: List[str] = field(default_factory=list)
    operators: List[BoundOperator] = field(default_factory=list)
    unprocessed_by_node_key: Optional[str] = None
    data_block_id: Optional[str] = None
    allow_cycle: bool = False

    def __str__(self):
        s = "Stream(\n"
        s += f"\tinputs={self.node_keys},\n"
        s += f"\tschemas={self.schema_keys},\n"
        s += f"\tstorages={self.storage_urls},\n"
        s += f"\toperators={self.operators},\n"
        s += ")"
        return s


def to_stream_builder(
    nodes: Union[NodeLike, List[NodeLike]] = None,
    schemas: List[SchemaLike] = None,
    storages: List[Storage] = None,
    schema: SchemaLike = None,
    storage: Storage = None,
    unprocessed_by: Node = None,
    data_block: Union[DataBlockMetadata, DataBlock, str] = None,
    allow_cycle: bool = False,
    operators: List[BoundOperator] = None,
) -> StreamBuilder:
    if schema is not None:
        assert schemas is None
        schemas = [schema]
    if storage is not None:
        assert storages is None
        storages = [storage]
    return StreamBuilder(
        StreamBuilderSerializable(
            node_keys=[ensure_node_key(n) for n in ensure_list(nodes)],
            schema_keys=[ensure_schema_key(n) for n in ensure_list(schemas)],
            storage_urls=ensure_list(storages),
            operators=ensure_list(operators),
            data_block_id=ensure_data_block_id(data_block),
            unprocessed_by_node_key=ensure_node_key(unprocessed_by)
            if unprocessed_by is not None
            else None,
            allow_cycle=allow_cycle,
        )
    )


stream = to_stream_builder


class StreamBuilder:
    """"""

    def __init__(
        self,
        filters: StreamBuilderSerializable = None,
    ):
        self._filters = filters or StreamBuilderSerializable()

    def __str__(self):
        return str(self._filters)

    def _base_query(self) -> Query:
        return Query(DataBlockMetadata).order_by(DataBlockMetadata.id)

    def get_query(self, ctx: RunContext, sess: Session) -> Query:
        q = self._base_query()
        if self._filters.node_keys is not None:
            q = self._filter_upstream(ctx, sess, q)
        if self._filters.schema_keys is not None:
            q = self._filter_schemas(ctx, sess, q)
        if self._filters.storage_urls is not None:
            q = self._filter_storages(ctx, sess, q)
        if self._filters.unprocessed_by_node_key is not None:
            q = self._filter_unprocessed(ctx, sess, q)
        if self._filters.data_block_id is not None:
            q = self._filter_data_block(ctx, sess, q)
        return q.with_session(sess)

    def clone(self, **kwargs) -> StreamBuilder:
        args = asdict(self._filters)
        args.update(**kwargs)
        return StreamBuilder(filters=StreamBuilderSerializable(**args))  # type: ignore

    def source_node_keys(self) -> List[str]:
        return self._filters.node_keys

    def filter_unprocessed(
        self, unprocessed_by: Node, allow_cycle=False
    ) -> StreamBuilder:
        return self.clone(
            unprocessed_by_node_key=unprocessed_by.key,
            allow_cycle=allow_cycle,
        )

    def _filter_unprocessed(
        self,
        ctx: RunContext,
        sess: Session,
        query: Query,
    ) -> Query:
        if not self._filters.unprocessed_by_node_key:
            return query
        if self._filters.allow_cycle:
            # Only exclude blocks processed as INPUT
            filter_clause = and_(
                DataBlockLog.direction == Direction.INPUT,
                SnapLog.node_key == self._filters.unprocessed_by_node_key,
            )
        else:
            # No block cycles allowed
            # Exclude blocks processed as INPUT and blocks outputted
            filter_clause = SnapLog.node_key == self._filters.unprocessed_by_node_key
        already_processed_drs = (
            Query(DataBlockLog.data_block_id)
            .join(SnapLog)
            .filter(filter_clause)
            .filter(DataBlockLog.invalidated == False)  # noqa
            .distinct()
        )
        return query.filter(not_(DataBlockMetadata.id.in_(already_processed_drs)))

    def get_upstream(self, g: Graph) -> List[Node]:
        return [g.get_node(c) for c in self._filters.node_keys]

    def filter_upstream(
        self, upstream: Union[NodeLike, List[NodeLike]]
    ) -> StreamBuilder:
        return self.clone(node_keys=[ensure_node_key(n) for n in ensure_list(upstream)])

    def _filter_upstream(
        self,
        ctx: RunContext,
        sess: Session,
        query: Query,
    ) -> Query:
        if not self._filters.node_keys:
            return query
        eligible_input_drs = (
            Query(DataBlockLog.data_block_id)
            .join(SnapLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                SnapLog.node_key.in_(self._filters.node_keys),
            )
            .filter(DataBlockLog.invalidated == False)  # noqa
            .distinct()
        )
        return query.filter(DataBlockMetadata.id.in_(eligible_input_drs))

    def get_schemas(self, env: Environment, sess: Session):
        return [env.get_schema(d, sess) for d in self._filters.schema_keys]

    def filter_schemas(self, schemas: List[SchemaLike]) -> StreamBuilder:
        return self.clone(
            schema_keys=[ensure_schema_key(s) for s in ensure_list(schemas)]
        )

    def _filter_schemas(self, ctx: RunContext, sess: Session, query: Query) -> Query:
        if not self._filters.schema_keys:
            return query
        return query.filter(
            DataBlockMetadata.nominal_schema_key.in_([d.key for d in self.get_schemas(ctx.env, sess)])  # type: ignore
        )

    def filter_schema(self, schema: SchemaLike) -> StreamBuilder:
        return self.filter_schemas(ensure_list(schema))

    def filter_storages(self, storages: List[Storage]) -> StreamBuilder:
        return self.clone(storage_urls=[s.url for s in storages])

    def _filter_storages(self, ctx: RunContext, sess: Session, query: Query) -> Query:
        if not self._filters.storage_urls:
            return query
        return query.join(StoredDataBlockMetadata).filter(
            StoredDataBlockMetadata.storage_url.in_(self._filters.storage_urls)  # type: ignore
        )

    def filter_storage(self, storage: Storage) -> StreamBuilder:
        return self.filter_storages(ensure_list(storage))

    def filter_data_block(
        self, data_block: Union[DataBlockMetadata, DataBlock, str]
    ) -> StreamBuilder:
        return self.clone(data_block_id=ensure_data_block_id(data_block))

    def _filter_data_block(self, ctx: RunContext, sess: Session, query: Query) -> Query:
        if not self._filters.data_block_id:
            return query
        return query.filter(DataBlockMetadata.id == self._filters.data_block_id)

    def get_operators(self) -> List[BoundOperator]:
        return self._filters.operators or []

    def apply_operator(self, op: BoundOperator) -> StreamBuilder:
        return self.clone(operators=(self.get_operators() + [op]))

    def is_unprocessed(
        self,
        ctx: RunContext,
        sess: Session,
        block: DataBlockMetadata,
        node: Node,
    ) -> bool:
        blocks = self.filter_unprocessed(node)
        q = blocks.get_query(ctx, sess)
        return q.filter(DataBlockMetadata.id == block.id).count() > 0

    def get_count(self, ctx: RunContext, sess: Session) -> int:
        return self.get_query(ctx, sess).count()

    def get_all(self, ctx: RunContext, sess: Session) -> List[DataBlockMetadata]:
        return self.get_query(ctx, sess).all()

    def as_managed_stream(
        self,
        ctx: RunContext,
        sess: Session,
        declared_schema: Optional[Schema] = None,
        declared_schema_translation: Optional[Dict[str, str]] = None,
    ) -> ManagedDataBlockStream:
        return ManagedDataBlockStream(
            ctx,
            sess,
            self,
            declared_schema=declared_schema,
            declared_schema_translation=declared_schema_translation,
        )


def block_as_stream_builder(data_block: DataBlockMetadata) -> StreamBuilder:
    return StreamBuilder(
        StreamBuilderSerializable(data_block_id=ensure_data_block_id(data_block))
    )


def block_as_stream(
    data_block: DataBlockMetadata,
    ctx: RunContext,
    sess: Session,
    declared_schema: Optional[Schema] = None,
    declared_schema_translation: Optional[Dict[str, str]] = None,
) -> DataBlockStream:
    stream = block_as_stream_builder(data_block)
    return stream.as_managed_stream(
        ctx, sess, declared_schema, declared_schema_translation
    )


# class ManagedDataBlockStream:
#     def __init__(
#         self,
#         ctx: RunContext,
#         sess: Session,
#         stream_builder: StreamBuilder,
#         declared_schema: Optional[Schema] = None,
#         declared_schema_translation: Optional[Dict[str, str]] = None,
#     ):
#         self.ctx = ctx
#         self.sess = sess
#         self.declared_schema = declared_schema
#         self.declared_schema_translation = declared_schema_translation
#         self._stream_builder = stream_builder
#         self._stream: Optional[Iterator[DataBlock]] = None
#         # self._build_stream()
#         # self._blocks: List[DataBlock] = list(self._build_stream(stream_builder))
#         # self._stream: Iterator[DataBlock] = self.log_emitted(self._blocks)
#         # self._emitted_blocks: List[DataBlockMetadata] = []
#         # self._emitted_managed_blocks: List[DataBlock] = []

#     def _build_stream(self):
#         query = self._stream_builder.get_query(self.ctx, self.sess)
#         stream = (b for b in query)
#         self._stream = self.as_managed_block(stream)
#         for op in self._stream_builder.get_operators():
#             self._stream = op.op_callable(self._stream, **op.kwargs)

#     def __iter__(self) -> Iterator[DataBlock]:
#         if self._stream is None:
#             self._build_stream()
#         assert self._stream is not None
#         return self._stream

#     def __next__(self) -> DataBlock:
#         if self._stream is None:
#             self._build_stream()
#         assert self._stream is not None
#         return next(self._stream)

#     def as_managed_block(
#         self, stream: Iterator[DataBlockMetadata]
#     ) -> Iterator[DataBlock]:
#         for db in stream:
#             if db.nominal_schema_key:
#                 schema_translation = get_schema_translation(
#                     self.ctx.env,
#                     self.sess,
#                     source_schema=db.nominal_schema(self.ctx.env, self.sess),
#                     target_schema=self.declared_schema,
#                     declared_schema_translation=self.declared_schema_translation,
#                 )
#             else:
#                 schema_translation = None
#             mdb = db.as_managed_data_block(
#                 self.ctx, self.sess, schema_translation=schema_translation
#             )
#             yield mdb

#     def consume(self) -> DataBlock:
#         db = next(self)
#         self.log_emitted(db)
#         return db

#     def reference(self) -> DataBlock:
#         # just don't do "unprocessed_by" step, right?
#         self = self.latest()
#         db = next(self)
#         # DO NOT log, just reference
#         # WELL, we need to log, but as a
#         return db

#     def latest(self) -> ManagedDataBlockStream:
#         from snapflow.core import operators

#         self.apply(operators.latest)  # Idempotent, so fine if latest already applied
#         return self

#     def next(self) -> ManagedDataBlockStream:
#         # This is no op, since it is default behavior
#         return self

#     def apply(self, operator: Operator, **kwargs):
#         if self._stream is not None:
#             raise Exception("Cannot apply operator after iterating stream")
#         bound = BoundOperator(op_callable=operator.op_callable, kwargs=kwargs)
#         self._stream_builder = self._stream_builder.apply_operator(bound)


class ManagedDataBlockStream:
    def __init__(
        self,
        ctx: RunContext,
        sess: Session,
        stream_builder: StreamBuilder,
        declared_schema: Optional[Schema] = None,
        declared_schema_translation: Optional[Dict[str, str]] = None,
    ):
        self.ctx = ctx
        self.sess = sess
        self.declared_schema = declared_schema
        self.declared_schema_translation = declared_schema_translation
        self._blocks: List[DataBlock] = list(self._build_stream(stream_builder))
        self._stream: Iterator[DataBlock] = self.log_emitted(self._blocks)
        self._emitted_blocks: List[DataBlockMetadata] = []
        self._emitted_managed_blocks: List[DataBlock] = []

    def _build_stream(self, stream_builder: StreamBuilder) -> Iterator[DataBlock]:
        query = stream_builder.get_query(self.ctx, self.sess)
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
            if db.nominal_schema_key:
                schema_translation = get_schema_translation(
                    self.ctx.env,
                    self.sess,
                    source_schema=db.nominal_schema(self.ctx.env, self.sess),
                    target_schema=self.declared_schema,
                    declared_schema_translation=self.declared_schema_translation,
                )
            else:
                schema_translation = None
            mdb = db.as_managed_data_block(
                self.ctx, self.sess, schema_translation=schema_translation
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
