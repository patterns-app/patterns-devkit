from __future__ import annotations

from textwrap import wrap
from typing import Dict, Optional

from loguru import logger
from pandas import DataFrame, concat
from snapflow.core.data_block import DataBlock
from snapflow.core.execution import SnapContext
from snapflow.core.snap import Snap
from snapflow.core.snap_interface import DeclaredSnapInterface
from snapflow.core.sql.sql_snap import SqlSnapWrapper, sql_snap
from snapflow.core.streams import Stream
from snapflow.core.typing.inference import conform_dataframe_to_schema
from snapflow.schema.base import Implementation, create_quick_schema
from snapflow.storage.data_formats.data_frame import DataFrameFormat
from snapflow.storage.data_formats.database_table_ref import DatabaseTableRefFormat
from snapflow.storage.db.utils import column_map, get_tmp_sqlite_db_url
from snapflow.testing.utils import (
    DataInput,
    produce_snap_output_for_static_input,
    str_as_dataframe,
)
from snapflow.utils.pandas import assert_dataframes_are_almost_equal
from snapflow.utils.typing import T


@Snap("dataframe_conform_to_schema", module="core")
def dataframe_conform_to_schema(
    ctx: SnapContext,
    input: DataBlock,
) -> DataFrame:
    env = ctx.run_context.env
    to_schema_key = ctx.get_param("schema")
    to_schema = env.get_schema(to_schema_key, ctx.execution_session.metadata_session)
    schema = input.nominal_schema
    translation = schema.get_translation_to(
        env, ctx.execution_session.metadata_session, other=to_schema
    )
    assert translation is not None
    df = input.as_dataframe()
    df = DataFrameFormat.apply_schema_translation(translation, df)
    fields = to_schema.field_names()
    cols = [c for c in df.columns if c in fields]
    return df[cols]


class SqlConformToSchema(SqlSnapWrapper):
    def get_compiled_sql(self, ctx: SnapContext, inputs: Dict[str, DataBlock]):
        input = inputs["input"]
        env = ctx.run_context.env
        to_schema_key = ctx.get_param("schema")
        to_schema = env.get_schema(
            to_schema_key, ctx.execution_session.metadata_session
        )
        schema = input.nominal_schema
        translation = schema.get_translation_to(
            env, ctx.execution_session.metadata_session, other=to_schema
        )
        assert translation is not None
        fields = to_schema.field_names()
        mapping = translation.as_dict()
        sql = super().get_compiled_sql(ctx, inputs=inputs)
        sql = column_map(
            f"({sql}) as __conformed",
            [c for c in schema.field_names() if c in fields or c in mapping],
            mapping,
        )
        return sql

    def get_interface(self) -> DeclaredSnapInterface:
        return dataframe_conform_to_schema.get_interface()


sql_conform_to_schema = sql_snap(
    name="sql_conform_to_schema",
    sql="select * from input",
    module="core",
    wrapper_cls=SqlConformToSchema,
)


def test_conform():
    from snapflow.modules import core

    TestSchemaA = create_quick_schema(
        "TestSchemaA", [("a", "Integer"), ("b", "Integer")], module_name="core"
    )
    TestSchemaB = create_quick_schema(
        "TestSchemaB",
        [("a", "Integer"), ("c", "Integer"), ("d", "Unicode")],
        implementations=[Implementation("TestSchemaA", {"b": "c"})],
        module_name="core",
    )

    core.add_schema(TestSchemaA)
    core.add_schema(TestSchemaB)

    input_data = """
        a,c,d
        1,2,i
        1,3,i
        1,4,i
        2,2,i
    """
    expected = """
        a,b
        1,2
        1,3
        1,4
        2,2
    """
    # expected_df = str_as_dataframe(expected, schema=core.schemas.CoreTestSchema)
    data_input = DataInput(input_data, schema=TestSchemaB)
    s = get_tmp_sqlite_db_url()
    for p in [
        dataframe_conform_to_schema,
        sql_conform_to_schema,
    ]:
        with produce_snap_output_for_static_input(
            p, input=data_input, target_storage=s, params={"schema": "TestSchemaA"}
        ) as db:
            expected_df = DataInput(expected, schema=TestSchemaA).as_dataframe(
                db.manager.ctx.env, db.manager.sess
            )
            df = db.as_dataframe()
            assert_dataframes_are_almost_equal(df, expected_df, schema=TestSchemaA)
