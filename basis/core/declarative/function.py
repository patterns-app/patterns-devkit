from __future__ import annotations
from collections import OrderedDict
from dataclasses import asdict, dataclass

import typing
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
from commonmodel.base import schema_like_to_key

import networkx as nx
from pydantic import Field
from pydantic.class_validators import root_validator
from basis.core.component import ComponentLibrary, global_library
from basis.core.declarative.base import FrozenPydanticBase, update
from basis.core.persistence.schema import is_generic
from commonmodel import Schema


DEFAULT_OUTPUT_NAME = "stdout"
DEFAULT_INPUT_NAME = "stdin"
DEFAULT_ERROR_NAME = "stderr"
DEFAULT_STATE_NAME = "state"


class BlockType(str, Enum):
    Record = "Record"
    Table = "Table"
    Generic = "Generic"


class IoBaseCfg(FrozenPydanticBase):
    name: str
    schema_like: Union[str, Schema] = Field(None, alias="schema")
    description: Optional[str] = None
    required: bool = True
    block_type: BlockType = BlockType.Record
    # stream: Optional[bool] = False
    data_format: Optional[str] = None
    is_error: bool = False
    is_state: bool = False

    @property
    def schema_key(self) -> Optional[str]:
        if self.schema_like is None:
            return None
        return schema_like_to_key(self.schema_like)

    # def resolve(self, lib: ComponentLibrary) -> IoBase:
    #     return update(self, _schema=lib.get_schema(self.schema))

    @property
    def is_generic(self) -> bool:
        if self.schema_like is None:
            return False
        return is_generic(self.schema_like)


def Record(
    name: str,
    schema: Union[str, Schema] = None,
    description: Optional[str] = None,
    required: bool = True,
    data_format: Optional[str] = None,
) -> IoBaseCfg:
    return IoBaseCfg(
        name=name,
        schema_like=schema,
        description=description,
        required=required,
        data_format=data_format,
        block_type=BlockType.Record,
    )


def Table(
    name: str,
    schema: Union[str, Schema] = None,
    description: Optional[str] = None,
    required: bool = True,
    data_format: Optional[str] = None,
) -> IoBaseCfg:
    return IoBaseCfg(
        name=name,
        schema_like=schema,
        description=description,
        required=required,
        data_format=data_format,
        block_type=BlockType.Table,
    )


def Generic(
    name: str,
    schema: Union[str, Schema] = None,
    description: Optional[str] = None,
    required: bool = True,
    data_format: Optional[str] = None,
) -> IoBaseCfg:
    return IoBaseCfg(
        name=name,
        schema_like=schema,
        description=description,
        required=required,
        data_format=data_format,
        block_type=BlockType.Generic,
    )


def is_record_like(obj: Any) -> bool:
    return isinstance(obj, dict)


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


class ParameterCfg(FrozenPydanticBase):
    name: str
    datatype: ParameterType
    required: bool = False
    default: Any = None
    description: str = ""


def Parameter(
    name: str,
    datatype: Union[ParameterType, str],
    required: bool = False,
    default: Any = None,
    description: str = "",
) -> ParameterCfg:
    return ParameterCfg(
        name=name,
        datatype=normalize_parameter_type(datatype),
        required=required,
        default=default,
        description=description,
    )


DEFAULT_INPUT_ANNOTATION = "Block"
DEFAULT_OUTPUT = Table(name=DEFAULT_OUTPUT_NAME,)
DEFAULT_OUTPUTS = {DEFAULT_OUTPUT_NAME: DEFAULT_OUTPUT}
DEFAULT_STATE_OUTPUT_NAME = "state"
DEFAULT_STATE_OUTPUT = None  # IoBase(
#     schema="core.State", name=DEFAULT_STATE_OUTPUT_NAME, is_default=False
# )
DEFAULT_ERROR_OUTPUT_NAME = "error"
DEFAULT_ERROR_OUTPUT = None


class FunctionInterfaceCfg(FrozenPydanticBase):
    inputs: typing.OrderedDict[str, IoBaseCfg] = Field(default_factory=OrderedDict)
    outputs: typing.OrderedDict[str, IoBaseCfg] = Field(default_factory=OrderedDict)
    parameters: typing.OrderedDict[str, ParameterCfg] = Field(
        default_factory=OrderedDict
    )
    stdin: Optional[str] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None

    @root_validator
    def check_single_input_stream(cls, values: Dict) -> Dict:
        inputs = values.get("inputs", {})
        assert (
            len([i for i in inputs.values() if i.block_type == BlockType.Record]) <= 1
        ), f"At most one input may be streaming. ({inputs})"
        return values

    @root_validator
    def check_generics(cls, values: Dict) -> Dict:
        outputs = values.get("outputs", {})
        generic_outputs = [
            o for o in outputs.values() if o.block_type == BlockType.Generic
        ]
        inputs = values.get("inputs", {})
        if generic_outputs and inputs:
            # We have a generic output and some inputs, one of those inputs must be generic too!
            generic_inputs = [
                o for o in inputs.values() if o.block_type == BlockType.Generic
            ]
            assert (
                generic_inputs
            ), f"Generic output block type found, but no respective generic input declared."
        return values

    def resolve(self, lib: ComponentLibrary) -> FunctionInterfaceCfg:
        d = self.dict()
        for n, i in self.inputs.items():
            d["inputs"][n] = i.resolve(lib)
        for n, i in self.outputs.items():
            d["outputs"][n] = i.resolve(lib)
        return FunctionInterfaceCfg(**d)

    def get_input(self, name: str) -> IoBaseCfg:
        return self.inputs[name]

    def get_single_input(self) -> IoBaseCfg:
        assert len(self.inputs) == 1, self.inputs
        return self.inputs[list(self.inputs)[0]]

    def get_single_streaming_input(self) -> IoBaseCfg:
        inpts = self.get_streaming_inputs()
        assert len(inpts) == 1, inpts
        return inpts[list(inpts)[0]]

    def get_streaming_inputs(self) -> Dict[str, IoBaseCfg]:
        return {n: i for n, i in self.inputs.items() if i.block_type == "Record"}

    def get_stdin_name(self) -> Optional[str]:
        if self.stdin:
            return self.stdin
        try:
            nonref = self.get_single_streaming_input()
            return nonref.name
        except AssertionError:
            pass
        try:
            inp = self.get_single_input()
            return inp.name
        except AssertionError:
            pass
        return None

    def get_default_output(self) -> Optional[IoBaseCfg]:
        if len(self.outputs) == 1:
            return self.outputs[list(self.outputs)[0]]
        return self.outputs.get(DEFAULT_OUTPUT_NAME)

    def get_all_schema_keys(self) -> List[str]:
        schemas = []
        for i in self.inputs.values():
            if i.schema_like is None:
                continue
            schemas.append(schema_like_to_key(i.schema_like))
        for o in self.outputs.values():
            if o.schema_like is None:
                continue
            schemas.append(schema_like_to_key(o.schema_like))
        # TODO: for flow
        return schemas


class FunctionCfg(FrozenPydanticBase):
    name: str
    python_path: str
    interface: Optional[FunctionInterfaceCfg] = None
    required_storage_classes: List[str] = []
    required_storage_engines: List[str] = []
    # package_absolute_path: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    # _original_object: Any = None

    @property
    def key(self) -> str:
        assert self.python_path
        return self.python_path
        # assert self.name and self.python_path
        # return f"{self.python_path}.{self.name}"

    def resolve(self, lib: ComponentLibrary) -> FunctionCfg:
        if self.interface:
            return update(self, interface=self.interface.resolve(lib))
        return self


# class FunctionPackageCfg(FrozenPydanticBase):
#     root_path: str
#     function: FunctionCfg
#     # local_vars: Dict = None
#     # root_module: ModuleType
#     tests: List[Dict] = []
#     function_python_name: Optional[str] = None
#     readme_name: str = "README.md"
#     # python_requirements_path: str = None
#     # docker_file_path: str = None


class FunctionSourceFileCfg(FrozenPydanticBase):
    name: str
    namespace: str
    source: str
    source_language: str = "python"
