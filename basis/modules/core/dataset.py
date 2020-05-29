from __future__ import annotations

from typing import List, Union

from pandas import DataFrame

from basis.core.data_function import (
    DataFunctionCallable,
    DataFunctionChain,
    DataFunctionLike,
)
from basis.core.data_resource import DataResource, DataSet, DataSetMetadata
from basis.core.runnable import DataFunctionContext
from basis.core.runtime import RuntimeClass
from basis.core.sql.data_function import sql_datafunction
from basis.utils.registry import T


def dataframe_accumulator(
    input: DataResource[T], this: DataResource[T] = None,
) -> DataFrame[T]:
    records = input.as_dataframe()
    if this is not None:
        previous = this.as_dataframe()
        records = previous.append(records)
    return records


sql_accumulator = sql_datafunction(
    """
    select:T * from input:T
    {% if inputs.this %}
    union all
    select * from this:Optional[T]
    {% endif %}
    """,
    "sql_accumulator",
)


dedupe_unique_keep_max_value = sql_datafunction(
    """
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
    "dedupe_unique_keep_max_value",
)


# TODO: only supported by postgres
dedupe_unique_keep_first_value = sql_datafunction(
    """
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
    "dedupe_unique_keep_first_value",
)


def as_dataset(key: str) -> DataFunctionCallable:
    def as_dataset(ctx: DataFunctionContext, input: DataResource[T]) -> DataSet[T]:
        ds = (
            ctx.execution_context.metadata_session.query(DataSetMetadata)
            .filter(DataSetMetadata.key == key)
            .first()
        )
        if ds is None:
            ds = DataSetMetadata(key=key, otype_uri=input.otype_uri)
        ds.data_resource_id = input.data_resource_id
        ctx.execution_context.add(ds)
        table = input.as_table()
        ctx.worker.execute_sql(f"drop view if exists {key}")  # TODO: downtime here while table swaps, how to handle?
        ctx.worker.execute_sql(f"create view {key} as select * from {table.table_name}")
        return ds

    as_dataset.runtime_class = RuntimeClass.DATABASE  # TODO: principled way to handle supported runtimes
    return as_dataset


# TODO: this is not "DataFunctionLike", so can't be indexed / treated as such until given a key
def accumulate_as_dataset(key: str) -> DataFunctionLike:
    return DataFunctionChain(
        f"accumulate_as_dataset_{key}",
        [sql_accumulator, dedupe_unique_keep_first_value, as_dataset(key)],
    )


DataSetAccumulator = accumulate_as_dataset
