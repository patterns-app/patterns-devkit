from commonmodel.base import AnySchema
from snapflow.core.module import SnapflowModule

module = SnapflowModule(
    "core",
    py_module_path=__file__,
    py_module_name=__name__,
)
module.add_schema(AnySchema)
# module.export()
# namespace = module.namespace
# all_functions = module.functions  # Shortcuts, for tooling
# all_schemas = module.schemas  # Shortcuts, for tooling
