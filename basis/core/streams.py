from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from sqlalchemy import and_, not_
from sqlalchemy.orm import Query

from basis.core.data_block import (
    DataBlockMetadata,
    DataSetMetadata,
    StoredDataBlockMetadata,
)
from basis.core.environment import Environment
from basis.core.function_node import (
    DataBlockLog,
    DataFunctionLog,
    Direction,
    FunctionNode,
)
from basis.core.storage import Storage
from basis.core.typing.object_type import ObjectType, ObjectTypeLike
from basis.utils.common import ensure_list

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from basis.core.runnable import ExecutionContext


class DataBlockStream:
    """
    """

    def __init__(
        self,
        upstream: Union[FunctionNode, List[FunctionNode]] = None,
        otypes: List[ObjectTypeLike] = None,
        storages: List[Storage] = None,
        otype: ObjectTypeLike = None,
        storage: Storage = None,
        unprocessed_by: FunctionNode = None,
        data_sets: List[str] = None,
        data_sets_only: bool = False,
        allow_cycle: bool = False,
        most_recent_first: bool = False,
    ):
        # TODO: ugly duplicate params (but like obvious/intuitive interface for singulars)
        if otype is not None:
            assert otypes is None
            otypes = [otype]
        if storage is not None:
            assert storages is None
            storages = [storage]
        # TODO: make all these private?
        self.upstream = upstream
        self.otypes = otypes
        self.storages = storages
        self.data_sets = data_sets
        self.data_sets_only = data_sets_only
        self.unprocessed_by = unprocessed_by
        self.allow_cycle = allow_cycle
        self.most_recent_first = most_recent_first

    def _base_query(self) -> Query:
        return Query(DataBlockMetadata)

    def get_query(self, ctx: ExecutionContext) -> Query:
        q = self._base_query()
        if self.upstream is not None:
            q = self._filter_upstream(ctx, q)
        if self.otypes is not None:
            q = self._filter_otypes(ctx, q)
        if self.storages is not None:
            q = self._filter_storages(ctx, q)
        if self.unprocessed_by is not None:
            q = self._filter_unprocessed(ctx, q)
        if self.data_sets is not None:
            q = self._filter_datasets(ctx, q)
        return q.with_session(ctx.metadata_session)

    def clone(self, **kwargs) -> DataBlockStream:
        args = dict(
            upstream=self.upstream,
            otypes=self.otypes,
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
        self, unprocessed_by: FunctionNode, allow_cycle=False
    ) -> DataBlockStream:
        return self.clone(unprocessed_by=unprocessed_by, allow_cycle=allow_cycle,)

    def _filter_unprocessed(self, ctx: ExecutionContext, query: Query,) -> Query:
        if not self.unprocessed_by:
            return query
        if self.allow_cycle:
            # Only exclude DRs processed as INPUT
            filter_clause = and_(
                DataBlockLog.direction == Direction.INPUT,
                DataFunctionLog.function_node_name == self.unprocessed_by.name,
            )
        else:
            # No DR cycles allowed
            # Exclude DRs processed as INPUT and DRs outputted
            filter_clause = (
                DataFunctionLog.function_node_name == self.unprocessed_by.name
            )
        already_processed_drs = (
            Query(DataBlockLog.data_block_id)
            .join(DataFunctionLog)
            .filter(filter_clause)
            .distinct()
        )
        return query.filter(not_(DataBlockMetadata.id.in_(already_processed_drs)))

    def get_upstream(self, env: Environment) -> List[FunctionNode]:
        nodes = ensure_list(self.upstream)
        if not nodes:
            return []
        return [env.get_node(c) for c in nodes]

    def filter_upstream(
        self, upstream: Union[FunctionNode, List[FunctionNode]]
    ) -> DataBlockStream:
        return self.clone(upstream=ensure_list(upstream),)

    def _filter_upstream(self, ctx: ExecutionContext, query: Query,) -> Query:
        if not self.upstream:
            return query
        eligible_input_drs = (
            Query(DataBlockLog.data_block_id)
            .join(DataFunctionLog)
            .filter(
                DataBlockLog.direction == Direction.OUTPUT,
                DataFunctionLog.function_node_name.in_(
                    [c.name for c in self.get_upstream(ctx.env)]
                ),
            )
            .distinct()
        )
        return query.filter(DataBlockMetadata.id.in_(eligible_input_drs))

    def get_otypes(self, env: Environment):
        dts = ensure_list(self.otypes)
        return [env.get_otype(d) for d in dts]

    def filter_otypes(self, otypes: List[ObjectTypeLike]) -> DataBlockStream:
        return self.clone(otypes=otypes)

    def _filter_otypes(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.otypes:
            return query
        # otype_names = []  # TODO: Fully qualified otype keys?
        return query.filter(
            DataBlockMetadata.otype_uri.in_([d.uri for d in self.get_otypes(ctx.env)])  # type: ignore
        )

    def filter_otype(self, otype: ObjectTypeLike) -> DataBlockStream:
        return self.filter_otypes(ensure_list(otype))

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

    # TODO: Does this work?
    def filter_dataset(self, dataset_name: Optional[str] = None) -> DataBlockStream:
        # TODO: support more than one
        names = None
        if dataset_name:
            names = [dataset_name]
        return self.clone(data_sets=names, data_sets_only=True)

    def _filter_datasets(self, ctx: ExecutionContext, query: Query) -> Query:
        if self.data_sets_only:
            query = query.join(DataSetMetadata)
        if not self.data_sets:
            return query
        return query.filter(DataSetMetadata.name.in_(self.data_sets))  # type: ignore

    def is_unprocessed(
        self, ctx: ExecutionContext, block: DataBlockMetadata, node: FunctionNode,
    ) -> bool:
        drs = self.filter_unprocessed(node)
        q = drs.get_query(ctx)
        return q.filter(DataBlockMetadata.id == block.id).exists()

    # def resolve_dependencies(self, env: Environment) -> List[FunctionNode]:
    #     from basis.core.graph import get_all_nodes_outputting_otype
    #
    #     deps: List[FunctionNode] = []
    #     if self.upstream:
    #         return self.get_upstream(env)
    #     elif self.otypes:
    #         if len(self.otypes) > 1:
    #             raise NotImplementedError("Mixed otype streams not supported atm")
    #         otype = self.get_otypes(env)[0]
    #
    #         for otype in self.get_otypes(env):
    #             streams.extend(get_all_nodes_outputting_otype(env, otype))
    #
    #     # if as_streams:
    #     #     streams = [ensure_data_stream(v) for v in streams]
    #     return deps
    #
    # def resolve_output_otype(self, env: Environment) -> ObjectType:
    #     pass

    # def get_data_stream_inputs(
    #     self, env: Environment, as_streams=False
    # ) -> List[DataBlockStreamable]:
    #     # TODO: ONLY handles explicit upstream and otype filters, and only with respect to NON-GENERIC data functions
    #     #   support for generic data functions would require compiling the otype graph statically.
    #     #   this may be tricky, eg do we need to handle
    #     #   heterogenuously typed inputs to a function?. Regardless, not how we do things currently --
    #     #   atm we bind inputs to concrete DataBlocks and resolve generics that way. Would require
    #     #   a rethink of that. Static compilation of type graph is probably desirable for other reasons, so likely
    #     #   the right long-term solution.
    #     from basis.core.graph import get_all_nodes_outputting_otype
    #
    #     streams: List[DataBlockStreamable] = []
    #     if self.upstream:
    #         streams = self.get_upstream(env)
    #     elif self.otypes:
    #         for otype in self.get_otypes(env):
    #             streams.extend(get_all_nodes_outputting_otype(env, otype))
    #
    #     if as_streams:
    #         streams = [ensure_data_stream(v) for v in streams]
    #     return streams

    def get_next(self, ctx: ExecutionContext) -> Optional[DataBlockMetadata]:
        order_by = DataBlockMetadata.updated_at
        if self.most_recent_first:
            order_by = order_by.desc()
        return (
            self.get_query(ctx).order_by(order_by).first()
        )  # TODO: should it be ordered by processed at? Also, DRs AREN'T updated LOL. That's the whole point

    def get_most_recent(self, ctx: ExecutionContext) -> Optional[DataBlockMetadata]:
        return (
            self.get_query(ctx).order_by(DataBlockMetadata.updated_at.desc()).first()
        )  # TODO: should it be ordered by processed at?

    def get_count(self, ctx: ExecutionContext) -> int:
        return self.get_query(ctx).count()

    def get_all(self, ctx: ExecutionContext) -> List[DataBlockMetadata]:
        return self.get_query(ctx).all()


DataBlockStreamable = Union[DataBlockStream, FunctionNode]
DataBlockStreamLike = Union[DataBlockStreamable, str]
FunctionNodeRawInput = Union[DataBlockStreamLike, Dict[str, DataBlockStreamLike]]
FunctionNodeInput = Union[DataBlockStreamable, Dict[str, DataBlockStreamable]]
InputStreams = Union[DataBlockStream, Dict[str, DataBlockStream]]
InputBlocks = Dict[str, DataBlockMetadata]


def ensure_data_stream(s: DataBlockStreamable) -> DataBlockStream:
    return s if isinstance(s, DataBlockStream) else s.as_stream()
