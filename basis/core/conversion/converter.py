from __future__ import annotations

import enum
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Set, Tuple, Type

import networkx as nx

from basis.core.data_format import DataFormat
from basis.core.data_resource import StoredDataResourceMetadata
from basis.core.storage_resource import StorageResource, StorageType

if TYPE_CHECKING:
    from basis.core.runnable import ExecutionContext


logger = logging.getLogger(__name__)


class ConversionCostLevel(enum.Enum):
    # TODO: make conversion a function of data size?
    NO_OP = 0
    MEMORY = 10
    DISK = 100
    OVER_WIRE = 1000

    def __lt__(self, other):
        return self.value < other.value


@dataclass(frozen=True)
class StorageFormat:
    storage_type: StorageType
    data_format: DataFormat

    def __str__(self):
        return f"{self.storage_type.display()}:{self.data_format.value}"


Conversion = Tuple[StorageFormat, StorageFormat]


@dataclass(frozen=True)
class ConversionEdge:
    converter_class: Type[Converter]
    conversion: Conversion


@dataclass(frozen=True)
class ConversionPath:
    conversions: List[ConversionEdge] = field(default_factory=list)

    def add(self, edge: ConversionEdge):
        self.conversions.append(edge)

    @property
    def total_cost(self) -> int:
        return sum(c.converter_class.cost_level.value for c in self.conversions)


class ConverterLookup:
    def __init__(self, eligible_storage_types: Set[StorageType] = None):
        self._lookup: Dict[Conversion, List[Type[Converter]]] = defaultdict(list)
        self._converters: List[Type[Converter]] = []
        self._graph = nx.MultiDiGraph()
        self.eligible_storage_types = eligible_storage_types

    def add(self, converter: Type[Converter]):
        self._converters.append(converter)
        for i in converter.supported_input_formats:
            if (
                self.eligible_storage_types
                and i.storage_type not in self.eligible_storage_types
            ):
                continue
            for o in converter.supported_output_formats:
                if (
                    self.eligible_storage_types
                    and o.storage_type not in self.eligible_storage_types
                ):
                    continue
                self._lookup[(i, o)].append(converter)
                self._graph.add_edge(
                    i, o, converter=converter, cost=converter.cost_level.value
                )

    def get_eligible(self, conversion: Conversion) -> List[Type[Converter]]:
        return self._lookup.get(conversion, [])

    def get_lowest_cost_path(
        self, conversion: Conversion, storage_resources: List[StorageResource] = None
    ) -> Optional[ConversionPath]:
        if storage_resources:
            return self.clone(
                set(s.storage_type for s in storage_resources)
            ).get_lowest_cost_path(conversion)
        # TODO: conversion paths
        try:
            path = nx.shortest_path(self._graph, *conversion, weight="cost")
        except nx.NetworkXNoPath:
            return None
        conversion_path = ConversionPath()
        for i in range(len(path) - 1):
            edge = (path[i], path[i + 1])
            cvtr_class = self.get_lowest_cost(edge)
            if cvtr_class:
                conversion_path.add(
                    ConversionEdge(converter_class=cvtr_class, conversion=edge)
                )
        return conversion_path

    def get_lowest_cost(self, conversion: Conversion) -> Optional[Type[Converter]]:
        converters = [(c.cost_level, c) for c in self.get_eligible(conversion)]
        if not converters:
            return None
        return min(converters)[1]

    def clone(self, eligible_storage_types: Set[StorageType] = None) -> ConverterLookup:
        # TODO: a bit overkill, hmmmm
        eligible_storage_types = eligible_storage_types or self.eligible_storage_types
        new_c = ConverterLookup(eligible_storage_types=eligible_storage_types)
        for c in self._converters:
            new_c.add(c)
        return new_c

    def display_graph(self):
        for n, adj in self._graph.adjacency():
            print(n)
            for d, attrs in adj.items():
                print("\t", d, attrs["converter"])


class Converter:
    supported_input_formats: Sequence[StorageFormat]
    supported_output_formats: Sequence[StorageFormat]
    cost_level: ConversionCostLevel

    def __init__(self, ctx: ExecutionContext):
        self.env = ctx.env
        self.ctx = ctx

    def convert(
        self,
        input_sdr: StoredDataResourceMetadata,
        output_storage: StorageResource,
        output_data_format: DataFormat,
    ) -> StoredDataResourceMetadata:
        if (
            input_sdr.data_format == output_data_format
            and input_sdr.storage_resource.storage_type == output_storage.storage_type
        ):
            # Nothing to do
            return input_sdr
        conversion = self.to_conversion(input_sdr, output_storage, output_data_format)
        if not self.is_supported(conversion):
            raise Exception(f"Not supported {conversion}")
        output_sdr = StoredDataResourceMetadata(  # type: ignore
            data_resource=input_sdr.data_resource,
            data_format=output_data_format,
            storage_resource_url=output_storage.url,
        )
        output_sdr = self.ctx.add(output_sdr)
        return self._convert(input_sdr, output_sdr)

    def _convert(
        self,
        input_sdr: StoredDataResourceMetadata,
        output_sdr: StoredDataResourceMetadata,
    ) -> StoredDataResourceMetadata:
        raise NotImplementedError

    def to_conversion(
        self,
        input_sdr: StoredDataResourceMetadata,
        output_storage: StorageResource,
        output_data_format: DataFormat,
    ):
        return (
            StorageFormat(
                input_sdr.storage_resource.storage_type, input_sdr.data_format,
            ),
            StorageFormat(output_storage.storage_type, output_data_format),
        )

    def is_supported(self, conversion: Conversion) -> bool:
        return (
            conversion[0] in self.supported_input_formats
            and conversion[1] in self.supported_output_formats
        )
