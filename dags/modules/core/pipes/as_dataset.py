from __future__ import annotations

from dags.core.data_block import DataBlock, DataSet, DataSetMetadata
from dags.core.pipe import pipe
from dags.core.runnable import PipeContext
from dags.utils.typing import T


@pipe("core.as_dataset_sql", compatible_runtimes="database")
def as_dataset_sql(ctx: PipeContext, input: DataBlock[T]) -> DataSet[T]:
    name = ctx.get_config_value("dataset_name")
    ds = (
        ctx.execution_context.metadata_session.query(DataSetMetadata)
        .filter(DataSetMetadata.name == name)
        .first()
    )
    if ds is None:
        ds = DataSetMetadata(
            name=name,
            expected_schema_key=input.expected_schema_key,
            realized_schema_key=input.realized_schema_key,
        )
    ds.data_block_id = input.data_block_id
    ctx.execution_context.add(ds)
    table = input.as_table()
    ctx.worker.execute_sql(
        f"drop view if exists {name}"
    )  # TODO: downtime here while table swaps, how to handle?
    ctx.worker.execute_sql(f"create view {name} as select * from {table.table_name}")
    return ds


@pipe("core.as_dataset", compatible_runtimes="python")
def as_dataset(ctx: PipeContext, input: DataBlock[T]) -> DataSet[T]:
    name = ctx.get_config_value("dataset_name")
    ds = (
        ctx.execution_context.metadata_session.query(DataSetMetadata)
        .filter(DataSetMetadata.name == name)
        .first()
    )
    if ds is None:
        ds = DataSetMetadata(
            name=name,
            expected_schema_key=input.expected_schema_key,
            realized_schema_key=input.realized_schema_key,
        )
    ds.data_block_id = input.data_block_id
    ctx.execution_context.add(ds)
    # TODO: How to create a "DataSet" in other storages / runtimes?
    # NOOP for now
    return ds
