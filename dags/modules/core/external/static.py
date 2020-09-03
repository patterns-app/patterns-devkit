from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Optional

import pandas as pd
from pandas import DataFrame

from dags import DataSet
from dags.core.data_formats import RecordsList, RecordsListGenerator
from dags.core.pipe import pipe
from dags.core.runnable import PipeContext
from dags.core.typing.object_type import ObjectTypeLike
from dags.utils.common import utcnow
from dags.utils.data import read_csv


@dataclass
class LocalResourceState:
    extracted: bool


@dataclass
class DataFrameResourceConfig:
    dataframe: DataFrame
    otype: ObjectTypeLike


@pipe(config_class=DataFrameResourceConfig, state_class=LocalResourceState)
def extract_dataframe(ctx: PipeContext,) -> DataSet:
    extracted = ctx.get_state("extracted")
    if extracted:
        # Just emit once
        return
    ctx.emit_state({"extracted": True})
    return ctx.get_config("dataframe")


@dataclass
class LocalCSVResourceConfig:
    path: str
    otype: ObjectTypeLike


@pipe(config_class=LocalCSVResourceConfig, state_class=LocalResourceState)
def extract_csv(ctx: PipeContext,) -> DataSet:
    extracted = ctx.get_state("extracted")
    if extracted:
        # Static resource, if already emitted, return
        return
    path = ctx.get_config("path")
    with open(path) as f:
        records = read_csv(f.readlines())
    ctx.emit_state({"extracted": True})
    return records


# def source_dataframe(ctx: PipeContext) -> DataFrame[Any]:
#     return ctx.config

# def static_source_resource(df: PipeCallable) -> PipeCallable:
#     """
#     Only run pipe once, since source data is static and only one output
#     """
#
#     def csr_key_from_node(node: ConfiguredPipe) -> str:
#         return f"_mock_source_resource_from_node_{node.key}"
#
#     @wraps(df)
#     def static_source(*args, **kwargs):
#         ctx: Any = args[0]  # PipeContext = args[0]
#         key = csr_key_from_node(ctx.node)
#         state = (
#             ctx._metadata_session.query(ConfiguredSourceResourceState)
#             .filter(ConfiguredSourceResourceState.configured_external_resource_key == key)
#             .first()
#         )
#         if state is not None:
#             # Already run once, don't run again
#             raise InputExhaustedException()
#         ret = df(*args, **kwargs)
#         state = ConfiguredSourceResourceState(
#             configured_external_resource_key=key, high_water_mark=utcnow(),
#         )
#         ctx._metadata_session.add(state)
#         return ret
#
#     # TODO: hmmm, we should have a unified interface for all Pipes
#     #   Probably has to be class / class wrapper?
#     #   wrapped either with decorator at declare time or ensured at runtime?
#     if hasattr(df, "get_interface"):
#
#         def call_(self, *args, **kwargs):
#             return static_source(*args, **kwargs)
#
#         df.__call__ = call_
#         return df
#
#         # class S:
#         #     def __call__(self, *args, **kwargs):
#         #         return static_source(*args, **kwargs)
#         #
#         #     def __getattr__(self, item):
#         #         return getattr(df, item)
#         #
#         # return S()
#
#     return static_source
