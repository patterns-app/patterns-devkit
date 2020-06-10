from __future__ import annotations

from basis.core.data_function import datafunction_chain
from basis.modules.core.functions.as_dataset import as_dataset
from basis.modules.core.functions.dedupe import dedupe_unique_keep_first_value

accumulate_as_dataset = datafunction_chain(
    name=f"accumulate_as_dataset",
    function_chain=["accumulator", dedupe_unique_keep_first_value, as_dataset],
)
