from basis.core.module import BasisModule

from .dataset import accumulate_as_dataset

module = BasisModule(
    "core",
    module_path=__file__,
    module_name=__name__,
    otypes=[],
    data_functions=[accumulate_as_dataset],
)
module.export()
