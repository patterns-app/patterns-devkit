from __future__ import annotations
from snapflow.core.persisted.pydantic import DataBlockWithStoredBlocksCfg
from snapflow.core.declarative.base import FrozenPydanticBase

from typing import TYPE_CHECKING, Dict, List, Optional, Set, Union

from commonmodel.base import Schema
from snapflow.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
)
from snapflow.core.declarative.graph import GraphCfg

if TYPE_CHECKING:
    from snapflow.core.streams import StreamBuilder


class NodeInputCfg(FrozenPydanticBase):
    name: str
    input: DataFunctionInputCfg
    input_node: Optional[GraphCfg] = None
    schema_translation: Optional[Dict[str, str]] = None

    def as_stream_builder(self) -> StreamBuilder:
        from snapflow.core.streams import StreamBuilder

        return StreamBuilder().filter_inputs([self.input_node.key])

    def as_bound_input(
        self,
        bound_block: DataBlockWithStoredBlocksCfg = None,
        bound_stream: List[DataBlockWithStoredBlocksCfg] = None,
    ) -> BoundInputCfg:
        from snapflow.core.function_interface_manager import BoundInput

        return BoundInputCfg(
            name=self.name,
            input=self.input,
            input_node=self.input_node,
            schema_translation=self.schema_translation,
            bound_block=bound_block,
            bound_stream=bound_stream,
        )


class BoundInputCfg(FrozenPydanticBase):
    name: str
    input: DataFunctionInputCfg
    input_node: Optional[GraphCfg] = None
    schema_translation: Optional[Dict[str, str]] = None
    bound_stream: Optional[List[DataBlockWithStoredBlocksCfg]] = None
    bound_block: Optional[DataBlockWithStoredBlocksCfg] = None

    def is_bound(self) -> bool:
        return self.bound_stream is not None or self.bound_block is not None

    def get_bound_block_property(self, prop: str):
        if self.bound_block:
            return getattr(self.bound_block, prop)
        if self.bound_stream:
            return getattr(self.bound_stream[0], prop)
        return None

    def get_bound_nominal_schema(self) -> Optional[Schema]:
        return self.get_bound_block_property("nominal_schema")

    def get_bound_realized_schema(self) -> Optional[Schema]:
        return self.get_bound_block_property("nominal_schema")

    @property
    def nominal_schema(self) -> Optional[Schema]:
        return self.get_bound_nominal_schema()

    @property
    def realized_schema(self) -> Optional[Schema]:
        return self.get_bound_realized_schema()

    @property
    def is_stream(self) -> bool:
        return self.input.is_stream


class BoundInterfaceCfg(FrozenPydanticBase):
    inputs: Dict[str, BoundInputCfg]
    interface: DataFunctionInterfaceCfg
