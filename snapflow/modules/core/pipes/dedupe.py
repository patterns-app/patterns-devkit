from __future__ import annotations

from pandas import DataFrame
from snapflow import DataBlock, pipe
from snapflow.core.node import DataBlockLog
from snapflow.core.sql.pipe import sql_pipe
from snapflow.testing.utils import (
    DataInput,
    get_tmp_sqlite_db_url,
    produce_pipe_output_for_static_input,
)
from snapflow.utils.pandas import assert_dataframes_are_almost_equal
from snapflow.utils.typing import T

# TODO: currently no-op when no unique columns specified.
#  In general any deduping on non-indexed columns will be costly.


# TODO: is there a generic minimal ANSI sql solution to dedupe keep newest? hmmmm
#  Does not appear to be, only hacks that require the sort column to be unique
sql_dedupe_unique_keep_newest_row = sql_pipe(
    name="sql_dedupe_unique_keep_newest_row",
    module="core",
    compatible_runtimes="postgres",  # TODO: compatible engines...
    sql="""
        select -- :DataBlock[T]
        {% if inputs.input.nominal_schema and inputs.input.nominal_schema.unique_on %}
            distinct on (
                {% for col in inputs.input.nominal_schema.unique_on %}
                    "{{ col }}"
                    {%- if not loop.last %},{% endif %}
                {% endfor %}
                )
        {% endif %}
            {% for col in inputs.input.realized_schema.fields %}
                "{{ col.name }}"
                {%- if not loop.last %},{% endif %}
            {% endfor %}

        from input -- :DataBlock[T]
        {% if inputs.input.nominal_schema.updated_at_field %}
        order by
            {% for col in inputs.input.nominal_schema.unique_on %}
                "{{ col }}",
            {% endfor %}
            "{{ inputs.input.nominal_schema.updated_at_field.name }}" desc
        {% endif %}
""",
)


@pipe("dataframe_dedupe_unique_keep_newest_row", module="core")
def dataframe_dedupe_unique_keep_newest_row(input: DataBlock[T]) -> DataFrame[T]:
    if input.nominal_schema is None or not input.nominal_schema.unique_on:
        return input.as_dataframe()  # TODO: make this a no-op
    records = input.as_dataframe()
    if input.nominal_schema.updated_at_field_name:
        records = records.sort_values(input.nominal_schema.updated_at_field_name)
    return records.drop_duplicates(input.nominal_schema.unique_on, keep="last")


def test_dedupe():
    from snapflow.modules import core

    input_data = """
        k1,k2,f1,f2,f3,f4
        1,2,abc,1.1,1,2012-01-01
        1,2,def,1.1,{"1":2},2012-01-02
        1,3,abc,1.1,2,2012-01-01
        1,4,,,"[1,2,3]",2012-01-01
        2,2,1.0,2.1,"[1,2,3]",2012-01-01
    """
    expected = """
        k1,k2,f1,f2,f3,f4
        1,2,def,1.1,{"1":2},2012-01-02
        1,3,abc,1.1,2,2012-01-01
        1,4,,,"[1,2,3]",2012-01-01
        2,2,1.0,2.1,"[1,2,3]",2012-01-01
    """
    # expected_df = str_as_dataframe(expected, schema=core.schemas.CoreTestSchema)
    data_input = DataInput(input_data, schema="CoreTestSchema", module=core)
    s = get_tmp_sqlite_db_url()
    for p in [
        # sql_dedupe_unique_keep_newest_row,
        dataframe_dedupe_unique_keep_newest_row,
    ]:
        db = produce_pipe_output_for_static_input(p, input=data_input, target_storage=s)
        expected_df = DataInput(
            expected, schema="CoreTestSchema", module=core
        ).as_dataframe(db.manager.ctx.env)
        df = db.as_dataframe()
        assert_dataframes_are_almost_equal(
            df, expected_df, schema=core.schemas.CoreTestSchema
        )
