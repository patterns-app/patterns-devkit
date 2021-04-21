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

from commonmodel.base import Schema, SchemaLike
from dcp.storage.base import Storage
from dcp.utils.common import ensure_list
from loguru import logger
from snapflow.core.data_block import (
    DataBlock,
    DataBlockMetadata,
    StoredDataBlockMetadata,
)
from snapflow.core.environment import Environment
from snapflow.core.function_interface_manager import get_schema_translation
from snapflow.core.graph import Graph
from snapflow.core.node import (
    DataBlockLog,
    DataFunctionLog,
    DeclaredNode,
    Direction,
    Node,
    NodeLike,
)
from sqlalchemy import and_, not_
from sqlalchemy.engine import Result
from sqlalchemy.orm import Query
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.selectable import Select

if TYPE_CHECKING:
    from snapflow.core.execution.executable import ExecutionContext
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
class StreamBuilderConfiguration:
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
        StreamBuilderConfiguration(
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
        filters: StreamBuilderConfiguration = None,
    ):
        self._filters = filters or StreamBuilderConfiguration()

    def __str__(self):
        return str(self._filters)

    def _base_query(self) -> Select:
        return select(DataBlockMetadata).order_by(DataBlockMetadata.id)

    def get_query(self, env: Environment) -> Select:
        q = self._base_query()
        if self._filters.node_keys is not None:
            q = self._filter_inputs(env, q)
        if self._filters.schema_keys is not None:
            q = self._filter_schemas(env, q)
        if self._filters.storage_urls is not None:
            q = self._filter_storages(env, q)
        if self._filters.unprocessed_by_node_key is not None:
            q = self._filter_unprocessed(env, q)
        if self._filters.data_block_id is not None:
            q = self._filter_data_block(env, q)
        return q.distinct()

    def get_query_result(self, env: Environment) -> Result:
        s = self.get_query(env)
        return env.md_api.execute(s)

    def clone(
        self,
        node_keys: List[str] = None,
        schema_keys: List[str] = None,
        storage_urls: List[str] = None,
        operators: List[BoundOperator] = None,
        unprocessed_by_node_key: Optional[str] = None,
        data_block_id: Optional[str] = None,
        allow_cycle: bool = False,
    ) -> StreamBuilder:
        f = self._filters
        sb = StreamBuilderConfiguration(
            node_keys=node_keys or f.node_keys,
            schema_keys=schema_keys or f.schema_keys,
            storage_urls=storage_urls or f.storage_urls,
            operators=operators or f.operators,
            data_block_id=data_block_id or f.data_block_id,
            unprocessed_by_node_key=unprocessed_by_node_key
            or f.unprocessed_by_node_key,
            allow_cycle=allow_cycle or f.allow_cycle,
        )
        return StreamBuilder(filters=sb)  # type: ignore

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
        env: Environment,
        query: Select,
    ) -> Select:
        if not self._filters.unprocessed_by_node_key:
            return query
        if self._filters.allow_cycle:
            # Only exclude blocks processed as INPUT
            filter_clause = and_(
                DataBlockLog.direction == Direction.INPUT,
                DataFunctionLog.node_key == self._filters.unprocessed_by_node_key,
            )
        else:
            # No block cycles allowed
            # Exclude blocks processed as INPUT and blocks outputted
            filter_clause = (
                DataFunctionLog.node_key == self._filters.unprocessed_by_node_key
            )
        already_processed_drs = (
            Query(DataBlockLog.data_block_id)
            .join(DataFunctionLog)
            .filter(filter_clause)
            .filter(DataBlockLog.invalidated == False)  # noqa
            .distinct()
        )
        return query.filter(not_(DataBlockMetadata.id.in_(already_processed_drs)))

    def get_inputs(self, g: Graph) -> List[Node]:
        return [g.get_node(c) for c in self._filters.node_keys]

    def filter_inputs(self, inputs: Union[NodeLike, List[NodeLike]]) -> StreamBuilder:
        return self.clone(node_keys=[ensure_node_key(n) for n in ensure_list(inputs)])

    def _filter_inputs(
        self,
        env: Environment,
        query: Select,
    ) -> Select:
        if not self._filters.node_keys:
            return query
        eligible_input_drs = (
            Query(DataBlockLog.data_block_id)
            .join(DataFunctionLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                DataFunctionLog.node_key.in_(self._filters.node_keys),
            )
            .filter(DataBlockLog.invalidated == False)  # noqa
            .distinct()
        )
        return query.filter(DataBlockMetadata.id.in_(eligible_input_drs))

    def get_schemas(self, env: Environment):
        return [env.get_schema(d) for d in self._filters.schema_keys]

    def filter_schemas(self, schemas: List[SchemaLike]) -> StreamBuilder:
        return self.clone(
            schema_keys=[ensure_schema_key(s) for s in ensure_list(schemas)]
        )

    def _filter_schemas(self, env: Environment, query: Select) -> Select:
        if not self._filters.schema_keys:
            return query
        return query.filter(
            DataBlockMetadata.nominal_schema_key.in_([d.key for d in self.get_schemas(env)])  # type: ignore
        )

    def filter_schema(self, schema: SchemaLike) -> StreamBuilder:
        return self.filter_schemas(ensure_list(schema))

    def filter_storages(self, storages: List[Storage]) -> StreamBuilder:
        return self.clone(storage_urls=[s.url for s in storages])

    def _filter_storages(self, env: Environment, query: Select) -> Select:
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

    def _filter_data_block(self, env: Environment, query: Select) -> Select:
        if not self._filters.data_block_id:
            return query
        return query.filter(DataBlockMetadata.id == self._filters.data_block_id)

    def get_operators(self) -> List[BoundOperator]:
        return self._filters.operators or []

    def apply_operator(self, op: BoundOperator) -> StreamBuilder:
        return self.clone(operators=(self.get_operators() + [op]))

    def is_unprocessed(
        self,
        env: Environment,
        block: DataBlockMetadata,
        node: Node,
    ) -> bool:
        blocks = self.filter_unprocessed(node)
        q = blocks.get_query(env)
        return q.filter(DataBlockMetadata.id == block.id).count() > 0

    def get_count(self, env: Environment) -> int:
        q = self.get_query(env)
        return env.md_api.count(q)

    def get_all(self, env: Environment) -> List[DataBlockMetadata]:
        return self.get_query_result(env).scalars()

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
    return StreamBuilder(
        StreamBuilderConfiguration(data_block_id=ensure_data_block_id(data_block))
    )


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
        result = stream_builder.get_query_result(self.ctx.env).scalars()
        stream = (b for b in result)
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
                    source_schema=db.nominal_schema(self.ctx.env),
                    target_schema=self.declared_schema,
                    declared_schema_translation=self.declared_schema_translation,
                )
            else:
                schema_translation = None
            mdb = db.as_managed_data_block(
                self.ctx.env, schema_translation=schema_translation
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
