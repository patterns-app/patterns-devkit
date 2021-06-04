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

    def get_bound_nominal_schema(self) -> Optional[str]:
        return self.get_bound_block_property("nominal_schema_key")

    def get_bound_realized_schema(self) -> Optional[str]:
        return self.get_bound_block_property("nominal_schema_key")

    @property
    def nominal_schema(self) -> Optional[str]:
        return self.get_bound_nominal_schema()

    @property
    def realized_schema(self) -> Optional[str]:
        return self.get_bound_realized_schema()

    @property
    def is_stream(self) -> bool:
        return self.input.is_stream


class BoundInterfaceCfg(FrozenPydanticBase):
    inputs: Dict[str, BoundInputCfg]
    interface: DataFunctionInterfaceCfg

    # def inputs_as_kwargs(self) -> Dict[str, Union[DataBlock, DataBlockStream]]:
    #     assert all([i.is_bound() for i in self.inputs.values()])
    #     return {
    #         i.name: i.bound_stream if i.is_stream else i.bound_block
    #         for i in self.inputs.values()
    #     }

    # def non_reference_bound_inputs(self) -> List[NodeInputCfg]:
    #     return [
    #         i
    #         for i in self.inputs.values()
    #         if i.bound_stream is not None and not i.input.is_reference
    #     ]

    # def resolve_nominal_output_schema(self) -> Optional[str]:
    #     output = self.interface.get_default_output()
    #     if not output:
    #         return None
    #     if not output.is_generic:
    #         return output.schema_key
    #     output_generic = output.schema_key
    #     for node_input in self.inputs.values():
    #         if not node_input.input.is_generic:
    #             continue
    #         if node_input.input.schema_key == output_generic:
    #             schema = node_input.get_bound_nominal_schema()
    #             # We check if None -- there may be more than one input with same generic, we'll take any that are resolvable
    #             if schema is not None:
    #                 return schema.key
    #     raise Exception(f"Unable to resolve generic '{output_generic}'")

    def resolve_nominal_output_schema(self) -> Optional[str]:
        output = self.interface.get_default_output()
        if not output:
            return None
        if not output.is_generic:
            return output.schema_key
        output_generic = output.schema_key
        for node_input in self.inputs.values():
            if not node_input.input.is_generic:
                continue
            if node_input.input.schema_key == output_generic:
                schema = node_input.get_bound_nominal_schema()
                # We check if None -- there may be more than one input with same generic, we'll take any that are resolvable
                if schema is not None:
                    return schema
        raise Exception(f"Unable to resolve generic '{output_generic}'")
