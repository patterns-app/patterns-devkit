# from __future__ import annotations
#
# from snapflow.core.pipe import Pipe, pipe_chain
# from snapflow.modules.core.pipes.accumulator import dataframe_accumulator, sql_accumulator
# from snapflow.modules.core.pipes.dedupe import (
#     dataframe_dedupe_unique_keep_newest_row,
#     sql_dedupe_unique_keep_newest_row,
# )
# from snapflow.testing.utils import (
#     DataInput,
#     get_tmp_sqlite_db_url,
#     produce_pipe_output_for_static_input,
# )
# from snapflow.utils.pandas import assert_dataframes_are_almost_equal
#
# sql_accumulate_as_dataset = pipe_chain(
#     name="sql_accumulate_as_dataset",
#     module="core",
#     pipe_chain=[sql_accumulator, sql_dedupe_unique_keep_newest_row],
# )
#
# dataframe_accumulate_as_dataset = pipe_chain(
#     name="dataframe_accumulate_as_dataset",
#     module="core",
#     pipe_chain=[
#         dataframe_accumulator,
#         dataframe_dedupe_unique_keep_newest_row,
#     ],
# )
#
#
# def test_accumulate_as_dataset():
#     from snapflow.modules import core
#
#     input_data = """
#         k1,k2,f1,f2,f3,f4
#         1,2,abc,1.1,1,2012-01-01
#         1,2,def,1.1,{"1":2},2012-01-02
#         1,3,abc,1.1,2,2012-01-01
#         1,4,,,"[1,2,3]",2012-01-01
#         2,2,1.0,2.1,"[1,2,3]",2012-01-01
#     """
#     expected = """
#         k1,k2,f1,f2,f3,f4
#         1,2,def,1.1,{"1":2},2012-01-02
#         1,3,abc,1.1,2,2012-01-01
#         1,4,,,"[1,2,3]",2012-01-01
#         2,2,1.0,2.1,"[1,2,3]",2012-01-01
#     """
#     data_input = DataInput(input_data, schema="CoreTestSchema", module=core)
#     s = get_tmp_sqlite_db_url()
#     for p in [
#         # sql_accumulate_as_dataset,  # TODO: Need sqlite support to test, or non-sqlite testing db
#         dataframe_accumulate_as_dataset
#     ]:
#         db = produce_pipe_output_for_static_input(
#             p, input=data_input, target_storage=s, config={"dataset_name": "test"}
#         )
#         env = db.manager.ctx.env
#         expected_df = DataInput(
#             expected, schema="CoreTestSchema", module=core
#         ).as_dataframe(env)
#         df = db.as_dataframe()
#         assert_dataframes_are_almost_equal(
#             df, expected_df, schema=core.schemas.CoreTestSchema
#         )
