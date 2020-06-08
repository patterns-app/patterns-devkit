from basis.core.component import ComponentType
from basis.core.external import (
    ExternalDataProvider,
    ExternalDataResource,
    ExternalProvider,
    ExternalResource,
)
from basis.core.module import BasisModule

p = ExternalDataProvider(
    name="test_provider", verbose_name="Test Provider", description="..."
)
r1 = ExternalDataResource(
    provider=p,
    name="TestExtResource",
    verbose_name="Test External Resource",
    description="...",
    otype="TestType",
    default_extractor=None,
)

module = BasisModule(
    "_test_module",
    py_module_path=__file__,
    py_module_name=__name__,
    otypes=["otypes/test_type.yml"],
    functions=["test_sql.sql"],
    providers=[p],
)
module.export()
