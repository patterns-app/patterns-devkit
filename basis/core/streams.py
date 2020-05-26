from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from sqlalchemy import and_, not_
from sqlalchemy.orm import Query

from basis.core.data_function import (
    ConfiguredDataFunction,
    DataFunctionLog,
    DataResourceLog,
    Direction,
)
from basis.core.data_resource import (
    DataResourceMetadata,
    DataSetMetadata,
    StoredDataResourceMetadata,
)
from basis.core.environment import Environment
from basis.core.object_type import ObjectTypeLike
from basis.core.storage import Storage
from basis.utils.common import ensure_list

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from basis.core.runnable import ExecutionContext


class DataResourceStream:
    """
    """

    def __init__(
        self,
        upstream: Union[ConfiguredDataFunction, List[ConfiguredDataFunction]] = None,
        otypes: List[ObjectTypeLike] = None,
        storages: List[Storage] = None,
        otype: ObjectTypeLike = None,
        storage: Storage = None,
        unprocessed_by: ConfiguredDataFunction = None,
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
        self.upstream = upstream
        self.otypes = otypes
        self.storages = storages
        self.data_sets = data_sets
        self.data_sets_only = data_sets_only
        self.unprocessed_by = unprocessed_by
        self.allow_cycle = allow_cycle
        self.most_recent_first = most_recent_first

    def _base_query(self) -> Query:
        return Query(DataResourceMetadata)

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

    def clone(self, **kwargs) -> DataResourceStream:
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
        return DataResourceStream(**args)  # type: ignore

    def filter_unprocessed(
        self, unprocessed_by: ConfiguredDataFunction, allow_cycle=False
    ) -> DataResourceStream:
        return self.clone(unprocessed_by=unprocessed_by, allow_cycle=allow_cycle,)

    def _filter_unprocessed(self, ctx: ExecutionContext, query: Query,) -> Query:
        if not self.unprocessed_by:
            return query
        if self.allow_cycle:
            # Only exclude DRs processed as INPUT
            filter_clause = and_(
                DataResourceLog.direction == Direction.INPUT,
                DataFunctionLog.configured_data_function_key == self.unprocessed_by.key,
            )
        else:
            # No DR cycles allowed
            # Exclude DRs processed as INPUT and DRs outputted
            filter_clause = (
                DataFunctionLog.configured_data_function_key == self.unprocessed_by.key
            )
        already_processed_drs = (
            Query(DataResourceLog.data_resource_id)
            .join(DataFunctionLog)
            .filter(filter_clause)
            .distinct()
        )
        return query.filter(not_(DataResourceMetadata.id.in_(already_processed_drs)))

    def get_upstream(self, env: Environment):
        cdfs = ensure_list(self.upstream)
        return [env.get_node(c) for c in cdfs]

    def filter_upstream(
        self, upstream: Union[ConfiguredDataFunction, List[ConfiguredDataFunction]]
    ) -> DataResourceStream:
        return self.clone(upstream=ensure_list(upstream),)

    def _filter_upstream(self, ctx: ExecutionContext, query: Query,) -> Query:
        if not self.upstream:
            return query
        eligible_input_drs = (
            Query(DataResourceLog.data_resource_id)
            .join(DataFunctionLog)
            .filter(
                DataResourceLog.direction == Direction.OUTPUT,
                DataFunctionLog.configured_data_function_key.in_(
                    [c.key for c in self.get_upstream(ctx.env)]
                ),
            )
            .distinct()
        )
        return query.filter(DataResourceMetadata.id.in_(eligible_input_drs))

    def get_otypes(self, env: Environment):
        dts = ensure_list(self.otypes)
        return [env.get_otype(d) for d in dts]

    def filter_otypes(self, otypes: List[ObjectTypeLike]) -> DataResourceStream:
        return self.clone(otypes=otypes)

    def _filter_otypes(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.otypes:
            return query
        # otype_keys = []  # TODO: Fully qualified otype keys?
        return query.filter(
            DataResourceMetadata.otype_uri.in_(
                [d.uri for d in self.get_otypes(ctx.env)]
            )
        )

    def filter_otype(self, otype: ObjectTypeLike) -> DataResourceStream:
        return self.filter_otypes(ensure_list(otype))

    def filter_storages(self, storages: List[Storage]) -> DataResourceStream:
        return self.clone(storages=storages)

    def _filter_storages(self, ctx: ExecutionContext, query: Query) -> Query:
        if not self.storages:
            return query
        return query.join(StoredDataResourceMetadata).filter(
            StoredDataResourceMetadata.storage_url.in_([s.url for s in self.storages])
        )

    def filter_storage(self, storage: Storage) -> DataResourceStream:
        return self.filter_storages(ensure_list(storage))

    # TODO: Does this work?
    def filter_dataset(self, dataset_key: Optional[str] = None) -> DataResourceStream:
        # TODO: support more than one
        keys = None
        if dataset_key:
            keys = [dataset_key]
        return self.clone(data_sets=keys, data_sets_only=True)

    def _filter_datasets(self, ctx: ExecutionContext, query: Query) -> Query:
        if self.data_sets_only:
            query = query.join(DataSetMetadata)
        if not self.data_sets:
            return query
        return query.filter(DataSetMetadata.key.in_(self.data_sets))

    def is_unprocessed(
        self,
        ctx: ExecutionContext,
        dr: DataResourceMetadata,
        cdf: ConfiguredDataFunction,
    ) -> bool:
        drs = self.filter_unprocessed(cdf)
        q = drs.get_query(ctx)
        return q.filter(DataResourceMetadata.id == dr.id).exists()

    def get_data_stream_inputs(
        self, env: Environment, as_streams=False
    ) -> List[DataResourceStreamable]:
        # TODO: ONLY handles explicit upstream and otype filters, and only with respect to NON-GENERIC data functions
        #   support for generic data functions would require compiling the otype graph statically.
        #   this may be tricky, eg do we need to handle
        #   heterogenuously typed inputs to a function?. Regardless, not how we do things currently --
        #   atm we bind inputs to concrete DataResources and resolve generics that way. Would require
        #   a rethink of that. Static compilation of type graph is probably desirable for other reasons, so likely
        #   the right long-term solution.
        from basis.core.graph import get_all_nodes_outputting_otype

        streams: List[DataResourceStreamable] = []
        if self.upstream:
            streams = self.get_upstream(env)
        elif self.otypes:
            for otype in self.get_otypes(env):
                streams.extend(get_all_nodes_outputting_otype(env, otype))

        if as_streams:
            streams = [ensure_data_stream(v) for v in streams]
        return streams

    def get_next(self, ctx: ExecutionContext) -> Optional[DataResourceMetadata]:
        order_by = DataResourceMetadata.updated_at
        if self.most_recent_first:
            order_by = order_by.desc()
        return (
            self.get_query(ctx).order_by(order_by).first()
        )  # TODO: should it be ordered by processed at? Also, DRs AREN'T updated LOL. That's the whole point

    def get_most_recent(self, ctx: ExecutionContext) -> Optional[DataResourceMetadata]:
        return (
            self.get_query(ctx).order_by(DataResourceMetadata.updated_at.desc()).first()
        )  # TODO: should it be ordered by processed at?

    def get_count(self, ctx: ExecutionContext) -> int:
        return self.get_query(ctx).count()

    def get_all(self, ctx: ExecutionContext) -> List[DataResourceMetadata]:
        return self.get_query(ctx).all()


DataResourceStreamable = Union[DataResourceStream, ConfiguredDataFunction]
InputResources = Dict[str, DataResourceMetadata]


def ensure_data_stream(s: DataResourceStreamable) -> DataResourceStream:
    return s if isinstance(s, DataResourceStream) else s.as_stream()
