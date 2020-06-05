from basis.core.module import BasisModule

from .dataset import *

module = BasisModule(
    "core",
    module_path=__file__,
    module_name=__name__,
    otypes=[],
    data_functions=[
        accumulate_as_dataset,
        as_dataset,
        dedupe_unique_keep_first_value,
        sql_accumulator,
        dataframe_accumulator,
    ],
)
module.export()
