from __future__ import annotations

from typing import List, Union

from pandas import DataFrame

from basis.core.data_block import DataBlock, DataSet, DataSetMetadata
from basis.core.data_function import (
    DataFunctionCallable,
    DataFunctionLike,
    datafunction_chain,
)
from basis.core.runnable import DataFunctionContext
from basis.core.runtime import RuntimeClass
from basis.core.sql.data_function import sql_datafunction
from basis.utils.registry import T


def dataframe_accumulator(
    input: DataBlock[T], this: DataBlock[T] = None,
) -> DataFrame[T]:
    records = input.as_dataframe()
    if this is not None:
        previous = this.as_dataframe()
        records = previous.append(records)
    return records


sql_accumulator = sql_datafunction(
    key="sql_accumulator",
    sql="""
    select:T * from input:T
    {% if inputs.this %}
    union all
    select * from this:Optional[T]
    {% endif %}
    """,
)


dedupe_unique_keep_max_value = sql_datafunction(
    key="dedupe_unique_keep_max_value",
    sql="""
select:T
distinct on (
    {% for col in inputs.input.otype.fields %}
        {% if col.name in inputs.input.otype.unique_on %}
            "{{ col.name }}"
        {% else %}
            {% if col.field_type.startswith("Bool") %}
                bool_or("{{ col.name }}") as "{{ col.name }}"
            {% else %}
                max("{{ col.name }}") as "{{ col.name }}"
            {% endif %}
        {% endif -%}
        {%- if not loop.last %},{% endif %}
    {% endfor %}
from input:T
group by 
    {% for col in inputs.input.otype.unique_on -%}
        "{{ col }}"
        {%- if not loop.last %},{% endif %}
    {% endfor %}
""",
)


# TODO: only supported by postgres
dedupe_unique_keep_first_value = sql_datafunction(
    key="dedupe_unique_keep_first_value",
    sql="""
select:T
distinct on (
    {% for col in inputs.input.otype.unique_on %}
        "{{ col }}"
        {%- if not loop.last %},{% endif %}
    {% endfor %}
    )
    {% for col in inputs.input.otype.fields %}
        "{{ col.name }}"
        {%- if not loop.last %},{% endif %}
    {% endfor %}
    
from input:T
""",
)


def as_dataset(key: str) -> DataFunctionCallable:
    def as_dataset(ctx: DataFunctionContext, input: DataBlock[T]) -> DataSet[T]:
        ds = (
            ctx.execution_context.metadata_session.query(DataSetMetadata)
            .filter(DataSetMetadata.key == key)
            .first()
        )
        if ds is None:
            ds = DataSetMetadata(key=key, otype_uri=input.otype_uri)
        ds.data_block_id = input.data_block_id
        ctx.execution_context.add(ds)
        table = input.as_table()
        ctx.worker.execute_sql(
            f"drop view if exists {key}"
        )  # TODO: downtime here while table swaps, how to handle?
        ctx.worker.execute_sql(f"create view {key} as select * from {table.table_name}")
        return ds

    return as_dataset


# TODO: this is not "DataFunctionLike", so can't be indexed / treated as such until given a key
# @datafunction("accumulate_as_dataset", configurable=True)
def accumulate_as_dataset(key: str) -> DataFunctionLike:
    return datafunction_chain(
        key=f"accumulate_as_dataset_{key}",
        function_chain=[
            sql_accumulator,
            dedupe_unique_keep_first_value,
            as_dataset(key),
        ],
    )


DataSetAccumulator = accumulate_as_dataset
