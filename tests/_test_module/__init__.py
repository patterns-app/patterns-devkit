from basis.core.module import BasisModule

module = BasisModule(
    "_test_module",
    py_module_path=__file__,
    py_module_name=__name__,
    otypes=["otypes/test_type.yml"],
    functions=["test_sql.sql"],
)
module.export()
