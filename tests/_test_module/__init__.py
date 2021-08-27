from __future__ import annotations

from pathlib import Path

from basis import Context
from basis.core.block import Block
from basis.utils.typing import T
from commonmodel.base import schema_from_yaml_file


def df1(ctx: Context) -> Block[T]:
    pass


TestSchema = schema_from_yaml_file(Path(__file__).parent / "schemas/TestSchema.yml")
# Shortcuts, for tooling and convenience
# namespace = module.namespace
# all_functions = module.functions
# all_schemas = module.schemas
