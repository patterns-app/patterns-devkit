from commonmodel.base import AnySchema
from snapflow.core.module import SnapflowModule


module = SnapflowModule("core", py_module_path=__file__, py_namespace=__name__,)
module.add_schema(AnySchema)
module.export()
