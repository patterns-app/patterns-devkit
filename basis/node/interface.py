from __future__ import annotations

import typing
from collections import OrderedDict
from dataclasses import asdict, dataclass
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from basis.configuration.base import FrozenPydanticBase, update
from commonmodel import Schema
from commonmodel.base import schema_like_to_key
from pydantic import Field
from pydantic.class_validators import root_validator, validator

DEFAULT_OUTPUT_NAME = "stdout"
DEFAULT_INPUT_NAME = "stdin"
DEFAULT_ERROR_NAME = "error"
DEFAULT_STATE_NAME = "state"


class OutputType(str, Enum):
    RecordStream = "RecordStream"
    Table = "Table"


class IoBase(FrozenPydanticBase):
    name: str
    schema_like: Union[str, Schema] = Field(None, alias="schema")
    description: Optional[str] = None
    required: bool = True
    output_type: OutputType = OutputType.RecordStream
    data_format: Optional[str] = None
    is_error: bool = False
    is_state: bool = False

    @property
    def schema_key(self) -> Optional[str]:
        if self.schema_like is None:
            return None
        return schema_like_to_key(self.schema_like)

    @property
    def is_system(self) -> bool:
        return self.is_error or self.is_state


def RecordStream(
    name: str,
    schema: Union[str, Schema] = None,
    description: Optional[str] = None,
    required: bool = True,
    data_format: Optional[str] = None,
) -> IoBase:
    return IoBase(
        name=name,
        schema_like=schema,
        description=description,
        required=required,
        data_format=data_format,
        output_type=OutputType.RecordStream,
    )


def Table(
    name: str,
    schema: Union[str, Schema] = None,
    description: Optional[str] = None,
    required: bool = True,
    data_format: Optional[str] = None,
) -> IoBase:
    return IoBase(
        name=name,
        schema_like=schema,
        description=description,
        required=required,
        data_format=data_format,
        output_type=OutputType.Table,
    )


class ParameterType(str, Enum):
    Text = "str"
    Boolean = "bool"
    Integer = "int"
    Float = "float"
    Date = "date"
    DateTime = "datetime"
    Json = "Dict"
    List = "List"


def normalize_parameter_type(pt: Union[str, ParameterType]) -> ParameterType:
    if isinstance(pt, ParameterType):
        return pt
    pt = dict(text="str", boolean="bool", number="float", integer="int",).get(
        pt.lower(), pt
    )
    return ParameterType(pt)


class Parameter(FrozenPydanticBase):
    name: str
    datatype: ParameterType
    required: bool = False
    default: Any = None
    description: str = ""

    @validator("datatype")
    def normalize_datatype(cls, value: Union[str, ParameterType]) -> ParameterType:
        return normalize_parameter_type(value)


DEFAULT_TABLE_OUTPUT = Table(name=DEFAULT_OUTPUT_NAME,)
# DEFAULT_TABLE_OUTPUTS = OrderedDict(DEFAULT_OUTPUT_NAME, DEFAULT_TABLE_OUTPUT])
DEFAULT_RECORD_OUTPUT = RecordStream(name=DEFAULT_OUTPUT_NAME,)
# DEFAULT_RECORD_OUTPUTS = {DEFAULT_OUTPUT_NAME: DEFAULT_RECORD_OUTPUT}
DEFAULT_STATE_OUTPUT_NAME = "state"
DEFAULT_STATE_OUTPUT = Table(name=DEFAULT_STATE_OUTPUT_NAME)
DEFAULT_STATE_INPUT = Table(name=DEFAULT_STATE_OUTPUT_NAME, required=False)
DEFAULT_ERROR_OUTPUT_NAME = "error"
DEFAULT_ERROR_STREAM_OUTPUT = RecordStream(name=DEFAULT_ERROR_OUTPUT_NAME)


class NodeInterface(FrozenPydanticBase):
    inputs: typing.OrderedDict[str, IoBase] = Field(default_factory=OrderedDict)
    outputs: typing.OrderedDict[str, IoBase] = Field(default_factory=OrderedDict)
    parameters: typing.OrderedDict[str, Parameter] = Field(default_factory=OrderedDict)

    @root_validator
    def check_single_input_stream(cls, values: Dict) -> Dict:
        inputs = values.get("inputs", {})
        assert (
            len(
                [i for i in inputs.values() if i.output_type == OutputType.RecordStream]
            )
            <= 1
        ), f"At most one input may be streaming. ({inputs})"
        return values

    def get_default_input(self) -> Optional[IoBase]:
        names = list(self.inputs)
        if not names:
            return None
        return self.inputs[names[0]]

    def get_default_output(self) -> Optional[IoBase]:
        names = list(self.outputs)
        if not names:
            return None
        return self.outputs[names[0]]

    @property
    def is_streaming(self) -> bool:
        "If either default input or output is streaming, then it's a streaming node"
        pi = self.get_default_input()
        if pi is not None and pi.output_type == OutputType.RecordStream:
            return True
        po = self.get_default_output()
        if po is not None and po.output_type == OutputType.RecordStream:
            return True
        return False

    @property
    def is_table(self) -> bool:
        return not self.is_streaming


def merge_interfaces(
    base: NodeInterface, update: NodeInterface, on_conflict_error: bool = True
) -> NodeInterface:
    inputs = base.inputs.copy()
    outputs = base.outputs.copy()
    parameters = base.parameters.copy()
    if on_conflict_error:
        assert set(inputs) & set(update.inputs) == set()
        assert set(outputs) & set(update.outputs) == set()
        assert set(parameters) & set(update.parameters) == set()
    inputs.update(update.inputs)
    outputs.update(update.outputs)
    parameters.update(update.parameters)
    return NodeInterface(inputs=inputs, outputs=outputs, parameters=parameters,)

