from __future__ import annotations

from typing import List, Union

from pandas import DataFrame

from basis.core.data_block import DataBlock, DataSet, DataSetMetadata
from basis.core.data_function import (
    DataFunctionCallable,
    DataFunctionLike,
    datafunction,
    datafunction_chain,
)
from basis.core.runnable import DataFunctionContext
from basis.core.runtime import RuntimeClass
from basis.core.sql.data_function import sql_datafunction
from basis.testing.functions import DataFunctionTestCase, TestCase
from basis.utils.typing import T


@datafunction(name="accumulator")  # , test_data="test_accumulator.yml")
def dataframe_accumulator(
    input: DataBlock[T], this: DataBlock[T] = None,
) -> DataFrame[T]:
    records = input.as_dataframe()
    if this is not None:
        previous = this.as_dataframe()
        records = previous.append(records)
    return records


sql_accumulator = sql_datafunction(
    name="accumulator",
    sql="""
    select:T * from input:T
    {% if inputs.this %}
    union all
    select * from this:Optional[T]
    {% endif %}
    """,
)


dedupe_unique_keep_max_value = sql_datafunction(
    name="dedupe_unique_keep_max_value",
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
    name="dedupe_unique_keep_first_value",
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


def as_dataset(ctx: DataFunctionContext, input: DataBlock[T]) -> DataSet[T]:
    name = ctx.config("dataset_name")
    ds = (
        ctx.execution_context.metadata_session.query(DataSetMetadata)
        .filter(DataSetMetadata.name == name)
        .first()
    )
    if ds is None:
        ds = DataSetMetadata(name=name, otype_uri=input.otype_uri)
    ds.data_block_id = input.data_block_id
    ctx.execution_context.add(ds)
    table = input.as_table()
    ctx.worker.execute_sql(
        f"drop view if exists {name}"
    )  # TODO: downtime here while table swaps, how to handle?
    ctx.worker.execute_sql(f"create view {name} as select * from {table.table_name}")
    return ds


accumulate_as_dataset = datafunction_chain(
    name=f"accumulate_as_dataset",
    function_chain=["accumulator", dedupe_unique_keep_first_value, as_dataset],
)


DataSetAccumulator = accumulate_as_dataset


TestCase(
    function="accumulator",
    tests=dict(
        test_intra_dedupe=dict(
            input="""
                k1,k2,f1,f2,f3
                1,2,abc,1.1,
                1,2,def,1.1,
                1,3,abc,1.1,
                1,4,,,
                2,2,1.0,2.1,"[1,2,3]"
            """,
            this="",
            output="""
                k1,k2,f1,f2,f3
                1,2,abc,1.1,
                1,3,abc,1.1,
                1,4,,,
                2,2,1.0,2.1,"[1,2,3]"
            """,
        )
    ),
)
