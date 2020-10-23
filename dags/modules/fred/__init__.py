from dags.core.module import DagsModule

from .pipes import extract_fred_observations

module = DagsModule(
    "fred",
    py_module_path=__file__,
    py_module_name=__name__,
    otypes=[],
    pipes=[extract_fred_observations,],
    tests=[],
)
module.export()
