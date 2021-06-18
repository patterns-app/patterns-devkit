from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional, Set, Union

from loguru import logger
from snapflow.core.data_block import as_managed
from snapflow.core.declarative.base import FrozenPydanticBase
from snapflow.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
    InputType,
)
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.persistence.pydantic import DataBlockWithStoredBlocksCfg


class NodeInputCfg(FrozenPydanticBase):
    name: str
    input: DataFunctionInputCfg
    input_node: Optional[GraphCfg] = None
    schema_translation: Optional[Dict[str, str]] = None

    def as_bound_input(
        self,
        bound_stream: List[DataBlockWithStoredBlocksCfg] = None,
    ) -> BoundInputCfg:

        return BoundInputCfg(
            name=self.name,
            input=self.input,
            input_node=self.input_node,
            schema_translation=self.schema_translation,
            bound_stream=bound_stream,
        )


class BoundInputCfg(FrozenPydanticBase):
    name: str
    input: DataFunctionInputCfg
    input_node: Optional[GraphCfg] = None
    schema_translation: Optional[Dict[str, str]] = None
    bound_stream: Optional[List[DataBlockWithStoredBlocksCfg]] = None

    def get_bound_input(
        self,
    ) -> Union[
        Iterator[DataBlockWithStoredBlocksCfg],
        List[DataBlockWithStoredBlocksCfg],
        DataBlockWithStoredBlocksCfg,
        None,
    ]:
        if self.input.is_stream:
            return self.get_bound_stream()
        if self.input.is_reference:
            return self.get_bound_reference_block()
        return self.get_bound_block_iterator()

    def get_bound_block_iterator(self) -> Iterator[DataBlockWithStoredBlocksCfg]:
        assert not self.input.is_stream and not self.input.is_reference
        if not self.bound_stream:
            return (_ for _ in [])
        return (b for b in self.bound_stream)

    def get_bound_stream(self) -> Optional[List[DataBlockWithStoredBlocksCfg]]:
        assert self.input.is_stream
        if not self.bound_stream:
            return None
        return self.bound_stream

    def get_bound_reference_block(self) -> Optional[DataBlockWithStoredBlocksCfg]:
        assert self.input.is_reference
        if not self.bound_stream:
            return None
        return self.bound_stream[-1]  # TODO: It will only be one right?

    def get_bound_block_property(self, prop: str):
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


class BoundInterfaceCfg(FrozenPydanticBase):
    inputs: Dict[str, BoundInputCfg]
    interface: DataFunctionInterfaceCfg

    def iter_as_function_kwarg_inputs(
        self,
    ) -> Iterator[
        Dict[
            str, Union[DataBlockWithStoredBlocksCfg, List[DataBlockWithStoredBlocksCfg]]
        ]
    ]:
        input_sources = {n: inpt.get_bound_input() for n, inpt in self.inputs.items()}
        while True:
            input_kwargs = {}
            for iname, src in input_sources.items():
                logger.debug(f"{iname}, {type(src)}, {src}")
                if isinstance(src, Iterator) and not self.inputs[iname].input.is_stream:
                    try:
                        block_input = next(src)
                    except StopIteration:
                        return
                else:
                    block_input = src
                    if isinstance(src, Iterable) and self.inputs[iname].input.is_stream:
                        # Ensure list
                        block_input = list(block_input)
                input_kwargs[iname] = block_input
            # print(
            #     {
            #         n: type(i[0]) if isinstance(i, list) else type(i)
            #         for n, i in input_kwargs.items()
            #     }
            # )
            yield input_kwargs
            if not self.has_consumable_input():
                break

    def has_consumable_input(self) -> bool:
        if any(
            [
                i.input.input_type == InputType.DataBlock and i.bound_stream
                for i in self.inputs.values()
            ]
        ):
            return True
        return False

    def non_reference_bound_inputs(self) -> List[BoundInputCfg]:
        return [
            i
            for i in self.inputs.values()
            if i.bound_stream is not None and not i.input.is_reference
        ]

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
        # TODO: extend to other output streams
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
