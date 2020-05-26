from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Type

from basis.core.data_function import DataFunction, Direction, ensure_datafunction
from basis.core.environment import Environment
from basis.core.module import BasisModule
from basis.core.object_type import ObjectType, ObjectTypeUri
from basis.core.external import ExternalProvider, ExternalResource

logger = logging.getLogger(__name__)


class ComponentType(Enum):
    ObjectType = "ObjectType"
    DataFunction = "DataFunction"
    ExternalProvider = "Provider"
    ExternalResource = "ExternalResource"


class ComponentTag(Enum):
    Fetch = "Fetch"
    Translate = "Translate"
    Sql = "SQL"
    Python = "Python"
    Metadata = "Metadata"


JSONable = Any  # TODO: proper json-able dict type?


@dataclass(frozen=True)
class ObjectTypeRelation:
    otype_uri: ObjectTypeUri
    direction: Direction


@dataclass(frozen=True)
class IndexableComponent:
    key: str
    module: str
    component_type: ComponentType
    verbose_name: str
    description: str
    otype_relations: Sequence[ObjectTypeRelation] = field(default_factory=list)
    keywords: Sequence[str] = field(default_factory=list)
    tags: Sequence[ComponentTag] = field(default_factory=list)
    parameters: Optional[Dict[str, JSONable]] = None
    importance: Optional[
        int
    ] = None  # Module-relative importance. higher int is higher importance. None is least important.
    repo_url: Optional[str] = None
    component: Optional[Any] = None


class ObjectTypeIndexer:
    def get_indexable_components(
        self, otype: ObjectType, m: BasisModule
    ) -> Iterable[IndexableComponent]:
        yield IndexableComponent(
            key=otype.key,
            module=m.key,
            component_type=ComponentType.ObjectType,
            verbose_name=otype.key,  # TODO?
            description=otype.description,
            keywords=[],  # TODO?
            component=otype,
        )


class DataFunctionIndexer:
    def get_relations(
        self, df: DataFunction, m: BasisModule
    ) -> List[ObjectTypeRelation]:
        env = Environment(create_metadata_storage=False)
        env.add_module(m)

        relations = []
        df = ensure_datafunction(df)
        dfi = df.get_interface()
        for input in dfi.inputs:
            dtr = ObjectTypeRelation(
                otype_uri=input.otype_uri(env), direction=Direction.INPUT
            )
            relations.append(dtr)
        if dfi.output:
            dtr = ObjectTypeRelation(
                otype_uri=dfi.output.otype_uri(env), direction=Direction.OUTPUT
            )
            relations.append(dtr)
        return relations

    def get_indexable_components(
        self, df: DataFunction, m: BasisModule
    ) -> Iterable[IndexableComponent]:
        relations = []
        try:
            relations = self.get_relations(df, m)
        except Exception as e:
            print(
                f"Couldn't generate ObjectType relations for DataFunction {df}: {e}"
            )  # TODO
        yield IndexableComponent(
            key=df.key,
            module=m.key,
            component_type=ComponentType.DataFunction,
            verbose_name=df.key,  # TODO?
            description=df.__doc__ or df.key,
            otype_relations=relations,
            keywords=[],  # TODO?
            component=df,
        )


class ExternalResourceIndexer:
    def get_relations(
        self, sr: ExternalResource, m: BasisModule
    ) -> List[ObjectTypeRelation]:
        from basis.core.data_function import Direction

        env = Environment(create_metadata_storage=False)
        env.add_module(m)

        return [
            ObjectTypeRelation(
                otype_uri=env.get_otype(sr.otype).uri, direction=Direction.OUTPUT,
            )
        ]

    def get_indexable_components(
        self, sr: ExternalResource, m: BasisModule
    ) -> Iterable[IndexableComponent]:
        yield IndexableComponent(
            key=sr.key,
            module=m.key,
            component_type=ComponentType.ExternalResource,
            verbose_name=sr.verbose_name,
            description=sr.description,
            otype_relations=self.get_relations(sr, m),
            keywords=[],  # TODO?
            component=sr,
        )


class ExternalProviderIndexer:
    def __init__(
        self,
        external_resource_indexer: Type[
            ExternalResourceIndexer
        ] = ExternalResourceIndexer,
    ):
        self.external_resource_indexer = external_resource_indexer()

    def get_indexable_components(
        self, s: ExternalProvider, m: BasisModule
    ) -> Iterable[IndexableComponent]:
        yield IndexableComponent(
            key=s.key,
            module=m.key,
            component_type=ComponentType.ExternalProvider,
            verbose_name=s.verbose_name,
            description=s.description,
            keywords=[],  # TODO?
            component=s,
        )
        for resource in s.resources:
            for ic in self.external_resource_indexer.get_indexable_components(
                resource, m
            ):
                yield ic


# class ComponentLibrary:
#     # TODO: use this inside env (instead of env implementing itself
#     def __init__(self):
#         self.registries: Dict[ComponentType, Registry] = {}
#
#     def add_module(self, module: Module):
#         pass
#
#     def add_component(self, component: IndexableComponent):
#         pass
#
#     def add_component_edge(
#         self,
#         component: IndexableComponent,
#         in_otype: ObjectTypeLike,
#         out_otype: ObjectTypeLike,
#     ):
#         pass

# ModeleIndexer:
#     def get_indexable_components(self) -> Iterable[IndexableComponent]:
#         dti = self.otype_indexer()
#         si = self.source_indexer()
#         dfi = self.data_function_indexer()
#         for otype in self.otypes.all():
#             for ic in dti.get_indexable_components(otype, self):
#                 yield ic
#         for s in self.sources.all():
#             for ic in si.get_indexable_components(s, self):
#                 yield ic
#         for df in self.data_functions.all():
#             for ic in dfi.get_indexable_components(df, self):
#                 yield ic
