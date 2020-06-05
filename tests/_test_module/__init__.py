from basis.core.module import BasisModule

module = BasisModule(
    "_test_module",
    module_path=__file__,
    module_name=__name__,
    otypes=["otypes/test_type.yml"],
    data_functions=["test_sql.sql"],
)
module.export()
