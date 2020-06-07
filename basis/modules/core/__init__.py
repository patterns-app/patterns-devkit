from basis.core.module import BasisModule

from .dataset import *

module = BasisModule(
    "core",
    py_module_path=__file__,
    py_module_name=__name__,
    otypes=[],
    functions=[
        accumulate_as_dataset,
        as_dataset,
        dedupe_unique_keep_first_value,
        sql_accumulator,
        dataframe_accumulator,
    ],
)
module.export()
