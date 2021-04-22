from __future__ import annotations

from pandas import DataFrame
from snapflow import DataBlock
from snapflow.core.function import datafunction
from snapflow.utils.typing import T

# TODO: currently no-op when no unique columns specified.
#  In general any deduping on non-indexed columns will be costly.


@datafunction(
    namespace="core",
    display_name="Dedupe DataFrame (keep latest)",
)
def dedupe_keep_latest(input: DataBlock[T]) -> DataFrame[T]:
    if input.nominal_schema is None or not input.nominal_schema.unique_on:
        return input.as_dataframe()  # TODO: make this a no-op
    records = input.as_dataframe()
    # TODO: what to sort by? bring this back? "modification ordering"
    # if input.nominal_schema.updated_at_field_name:
    #     records = records.sort_values(input.nominal_schema.updated_at_field_name)
    return records.drop_duplicates(input.nominal_schema.unique_on, keep="last")


# input_data = """
#     k1,k2,f1,f2,f3,f4
#     1,2,abc,1.1,1,2012-01-01
#     1,2,def,1.1,{"1":2},2012-01-02
#     1,3,abc,1.1,2,2012-01-01
#     1,4,,,"[1,2,3]",2012-01-01
#     2,2,1.0,2.1,"[1,2,3]",2012-01-01
# """
# expected = """
#     k1,k2,f1,f2,f3,f4
#     1,2,def,1.1,{"1":2},2012-01-02
#     1,3,abc,1.1,2,2012-01-01
#     1,4,,,"[1,2,3]",2012-01-01
#     2,2,1.0,2.1,"[1,2,3]",2012-01-01
# """
