from __future__ import annotations

from dataclasses import dataclass

from pandas import DataFrame

from dags import RecordsList
from dags.core.data_block import DataRecordsObject, as_records
from dags.core.pipe import pipe
from dags.core.runnable import PipeContext
from dags.core.typing.object_schema import ObjectSchemaLike
from dags.utils.data import read_csv


@dataclass
class LocalExtractState:
    extracted: bool


@dataclass
class ExtractDataFrameConfig:
    dataframe: DataFrame
    schema: ObjectSchemaLike


@pipe(
    "extract_dataframe",
    module="core",
    config_class=ExtractDataFrameConfig,
    state_class=LocalExtractState,
)
def extract_dataframe(
    ctx: PipeContext,
) -> DataRecordsObject:
    extracted = ctx.get_state_value("extracted")
    if extracted:
        # Just emit once
        return  # TODO: typing fix here?
    ctx.emit_state_value("extracted", True)
    schema = ctx.get_config_value("schema")
    df = ctx.get_config_value("dataframe")
    return as_records(df, schema=schema)


@dataclass
class ExtractLocalCSVConfig:
    path: str
    schema: ObjectSchemaLike


@pipe(
    "extract_csv",
    module="core",
    config_class=ExtractLocalCSVConfig,
    state_class=LocalExtractState,
)
def extract_csv(
    ctx: PipeContext,
) -> DataRecordsObject:
    extracted = ctx.get_state_value("extracted")
    if extracted:
        # Static resource, if already emitted, return
        return
    path = ctx.get_config_value("path")
    with open(path) as f:
        records = read_csv(f.readlines())
    ctx.emit_state_value("extracted", True)
    schema = ctx.get_config_value("schema")
    return as_records(records, data_format=RecordsList, schema=schema)
