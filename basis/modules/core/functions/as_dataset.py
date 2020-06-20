from __future__ import annotations

from basis.core.data_block import DataBlock, DataSet, DataSetMetadata
from basis.core.data_function import datafunction
from basis.core.runnable import DataFunctionContext
from basis.utils.typing import T


@datafunction("as_dataset", compatible_runtimes="database")
def as_dataset(ctx: DataFunctionContext, input: DataBlock[T]) -> DataSet[T]:
    name = ctx.config("dataset_name")
    ds = (
        ctx.execution_context.metadata_session.query(DataSetMetadata)
        .filter(DataSetMetadata.name == name)
        .first()
    )
    if ds is None:
        ds = DataSetMetadata(
            name=name,
            expected_otype_uri=input.expected_otype_uri,
            realized_otype_uri=input.realized_otype_uri,
        )
    ds.data_block_id = input.data_block_id
    ctx.execution_context.add(ds)
    table = input.as_table()
    ctx.worker.execute_sql(
        f"drop view if exists {name}"
    )  # TODO: downtime here while table swaps, how to handle?
    ctx.worker.execute_sql(f"create view {name} as select * from {table.table_name}")
    return ds
