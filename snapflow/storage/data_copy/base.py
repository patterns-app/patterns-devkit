from __future__ import annotations

import enum
import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import networkx as nx
from loguru import logger
from snapflow.schema.base import AnySchema, Schema
from snapflow.storage.data_formats import DataFormat
from snapflow.storage.data_formats.base import DataFormatBase
from snapflow.storage.data_formats.database_cursor import DatabaseCursorFormat
from snapflow.storage.data_formats.database_table import DatabaseTableFormat
from snapflow.storage.data_formats.database_table_ref import DatabaseTableRefFormat
from snapflow.storage.data_formats.records import RecordsFormat
from snapflow.storage.data_records import as_records
from snapflow.storage.storage import (
    DatabaseStorageClass,
    PythonStorageApi,
    PythonStorageClass,
    Storage,
    StorageApi,
    StorageClass,
    StorageEngine,
)
from snapflow.utils.registry import global_registry
from sqlalchemy.orm.session import Session

CostFunction = Callable[[int], int]
BUFFER_SIZE = 100


@dataclass(frozen=True)
class DataCopyCost:
    # TODO: we're not really using the cost parameter n
    # Maybe easier to just assume data n >> buffer n and nothing else matters?
    wire_cost: CostFunction
    memory_cost: CostFunction
    time_cost: CostFunction = (
        lambda n: n
    )  # Always the same? this is just dumb data copying

    def total_cost(self, n: int) -> int:
        return self.wire_cost(n) + self.time_cost(n) + self.memory_cost(n)

    def __add__(self, other: DataCopyCost) -> DataCopyCost:
        return DataCopyCost(
            wire_cost=lambda n: self.wire_cost(n) + other.wire_cost(n),
            memory_cost=lambda n: self.memory_cost(n) + other.memory_cost(n),
            time_cost=lambda n: self.time_cost(n) + other.time_cost(n),
        )


NoOpCost = DataCopyCost(
    wire_cost=lambda n: 0, memory_cost=lambda n: 0, time_cost=lambda n: 0
)
BufferToBufferCost = DataCopyCost(
    wire_cost=lambda n: 0, memory_cost=lambda n: BUFFER_SIZE
)
MemoryToBufferCost = DataCopyCost(wire_cost=lambda n: 0, memory_cost=lambda n: n)
MemoryToMemoryCost = DataCopyCost(wire_cost=lambda n: 0, memory_cost=lambda n: n)
DiskToBufferCost = DataCopyCost(
    wire_cost=lambda n: n, memory_cost=lambda n: BUFFER_SIZE
)
DiskToMemoryCost = DataCopyCost(wire_cost=lambda n: n, memory_cost=lambda n: n)
NetworkToMemoryCost = DataCopyCost(
    wire_cost=(
        lambda n: n * 5
    ),  # What is this factor in practice? What's a good default (think S3 vs local SSD?)
    memory_cost=lambda n: n,
)
NetworkToBufferCost = DataCopyCost(
    wire_cost=(lambda n: n * 5), memory_cost=lambda n: BUFFER_SIZE
)


@dataclass(frozen=True)
class StorageFormat:
    storage_engine: Type[StorageEngine]
    data_format: DataFormat

    def __str__(self):
        return f"{self.storage_engine.__name__}:{self.data_format.__name__}"


@dataclass(frozen=True)
class Conversion:
    from_storage_format: StorageFormat
    to_storage_format: StorageFormat


@dataclass(frozen=True)
class ConversionEdge:
    copier: DataCopier
    conversion: Conversion


@dataclass(frozen=True)
class ConversionPath:
    conversions: List[ConversionEdge] = field(default_factory=list)
    expected_record_count: int = 10000

    def add(self, edge: ConversionEdge):
        self.conversions.append(edge)

    def __len__(self) -> int:
        return len(self.conversions)

    @property
    def total_cost(self) -> int:
        return sum(
            c.copier.cost.total_cost(self.expected_record_count)
            for c in self.conversions
        )


CopierCallabe = Callable[
    [
        str,
        str,
        Conversion,
        StorageApi,
        StorageApi,
        Schema,
    ],
    None,
]


@dataclass(frozen=True)
class DataCopier:
    cost: DataCopyCost
    copier_function: CopierCallabe
    from_storage_classes: Optional[List[Type[StorageClass]]] = None
    from_storage_engines: Optional[List[Type[StorageEngine]]] = None
    from_data_formats: Optional[List[DataFormat]] = None
    to_storage_classes: Optional[List[Type[StorageClass]]] = None
    to_storage_engines: Optional[List[Type[StorageEngine]]] = None
    to_data_formats: Optional[List[DataFormat]] = None

    def copy(
        self,
        from_name: str,
        to_name: str,
        conversion: Conversion,
        from_storage_api: StorageApi,
        to_storage_api: StorageApi,
        schema: Schema = AnySchema,
    ):
        self.copier_function(
            from_name, to_name, conversion, from_storage_api, to_storage_api, schema
        )

    __call__ = copy

    def can_handle_from(self, from_storage_format: StorageFormat) -> bool:
        if self.from_storage_classes:
            if (
                from_storage_format.storage_engine.storage_class
                not in self.from_storage_classes
            ):
                return False
        if self.from_storage_engines:
            if from_storage_format.storage_engine not in self.from_storage_engines:
                return False
        if self.from_data_formats:
            if from_storage_format.data_format not in self.from_data_formats:
                return False
        return True

    def can_handle_to(self, to_storage_format: StorageFormat) -> bool:
        if self.to_storage_classes:
            if (
                to_storage_format.storage_engine.storage_class
                not in self.to_storage_classes
            ):
                return False
        if self.to_storage_engines:
            if to_storage_format.storage_engine not in self.to_storage_engines:
                return False
        if self.to_data_formats:
            if to_storage_format.data_format not in self.to_data_formats:
                return False
        return True

    def can_handle(self, conversion: Conversion) -> bool:
        return self.can_handle_from(
            conversion.from_storage_format
        ) and self.can_handle_to(conversion.to_storage_format)


all_data_copiers = []


def datacopy(
    cost: DataCopyCost,
    from_storage_classes: Optional[List[Type[StorageClass]]] = None,
    from_storage_engines: Optional[List[Type[StorageEngine]]] = None,
    from_data_formats: Optional[List[DataFormat]] = None,
    to_storage_classes: Optional[List[Type[StorageClass]]] = None,
    to_storage_engines: Optional[List[Type[StorageEngine]]] = None,
    to_data_formats: Optional[List[DataFormat]] = None,
    unregistered: bool = False,
):
    def f(copier_function: CopierCallabe) -> DataCopier:
        dc = DataCopier(
            copier_function=copier_function,
            cost=cost,
            from_storage_classes=from_storage_classes,
            from_storage_engines=from_storage_engines,
            from_data_formats=from_data_formats,
            to_storage_classes=to_storage_classes,
            to_storage_engines=to_storage_engines,
            to_data_formats=to_data_formats,
        )
        if not unregistered:
            all_data_copiers.append(dc)
        return dc

    return f


class CopyLookup:
    def __init__(
        self,
        copiers: Iterable[DataCopier],
        available_storage_engines: Set[Type[StorageEngine]] = None,
        available_data_formats: Iterable[DataFormat] = None,
        expected_record_count: int = 10000,
    ):
        self._lookup: Dict[Conversion, List[DataCopier]] = defaultdict(list)
        self._copiers: Iterable[DataCopier] = copiers
        self.available_data_formats = available_data_formats
        self.available_storage_engines = available_storage_engines
        self.available_storage_formats = self._get_all_available_formats()
        self.expected_record_count = expected_record_count  # TODO: hmmmmm
        self._graph = self._build_copy_graph(expected_record_count)

    def _get_all_available_formats(self) -> List[StorageFormat]:
        fmts = []
        for fmt in self.available_data_formats:
            for eng in self.available_storage_engines:
                for supported_fmt in eng.get_supported_formats():
                    if issubclass(fmt, supported_fmt):
                        fmts.append(StorageFormat(eng, fmt))
        return fmts

    def _build_copy_graph(self, expected_record_count: int) -> nx.MultiDiGraph:
        g = nx.MultiDiGraph()
        for c in self._copiers:
            for from_fmt in self.available_storage_formats:
                if c.can_handle_from(from_fmt):
                    for to_fmt in self.available_storage_formats:
                        if c.can_handle_to(to_fmt):
                            g.add_edge(
                                from_fmt,
                                to_fmt,
                                copier=c,
                                cost=c.cost.total_cost(expected_record_count),
                            )
                            self._lookup[Conversion(from_fmt, to_fmt)].append(c)
        return g

    def get_capable_copiers(self, conversion: Conversion) -> List[DataCopier]:
        return self._lookup.get(conversion, [])

    def get_lowest_cost_path(self, conversion: Conversion) -> Optional[ConversionPath]:
        try:
            path = nx.shortest_path(
                self._graph,
                conversion.from_storage_format,
                conversion.to_storage_format,
                weight="cost",
            )
        except nx.NetworkXNoPath:
            return None
        conversion_path = ConversionPath(
            expected_record_count=self.expected_record_count
        )
        for i in range(len(path) - 1):
            edge = Conversion(path[i], path[i + 1])
            copier = self.get_lowest_cost(edge)
            if copier:
                conversion_path.add(ConversionEdge(copier=copier, conversion=edge))
            else:
                return None
        return conversion_path

    def get_lowest_cost(self, conversion: Conversion) -> Optional[DataCopier]:
        converters = [
            (c.cost.total_cost(self.expected_record_count), random.random(), c)
            for c in self.get_capable_copiers(conversion)
        ]
        if not converters:
            return None
        return min(converters)[2]

    def display_graph(self):
        for n, adj in self._graph.adjacency():
            print(n)
            for d, attrs in adj.items():
                print("\t", d, attrs["converter"])


def get_datacopy_lookup(
    copiers: Iterable[DataCopier] = None,
    available_storage_engines: Set[Type[StorageEngine]] = None,
    available_data_formats: Iterable[DataFormat] = None,
    expected_record_count: int = 10000,
) -> CopyLookup:
    return CopyLookup(
        copiers=copiers or all_data_copiers,
        available_storage_engines=available_storage_engines
        or set(global_registry.all(StorageEngine)),
        available_data_formats=available_data_formats
        or list(global_registry.all(DataFormatBase)),
        expected_record_count=expected_record_count,
    )
