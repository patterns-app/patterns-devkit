from __future__ import annotations

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

import networkx as nx
from commonmodel import Schema
from dcp.utils.common import remove_dupes
from loguru import logger
from pydantic import Field
from snapflow.core.component import ComponentLibrary, global_library
from snapflow.core.data_block import Reference
from snapflow.core.declarative.base import FrozenPydanticBase
from snapflow.core.schema import is_generic

if TYPE_CHECKING:
    from snapflow.core.streams import StreamLike


DEFAULT_OUTPUT_NAME = "stdout"
DEFAULT_INPUT_NAME = "stdin"
DEFAULT_ERROR_NAME = "stderr"
DEFAULT_STATE_NAME = "state"


class InputType(str, Enum):
    DataBlock = "DataBlock"
    Reference = "Reference"
    Stream = "Stream"
    SelfReference = "SelfReference"
    Consumable = "Consumable"


class ParameterType(str, Enum):
    Text = "str"
    Boolean = "bool"
    Integer = "int"
    Float = "float"
    Date = "date"
    DateTime = "datetime"
    Json = "Dict"
    List = "List"


class Parameter(FrozenPydanticBase):
    name: str
    datatype: str
    required: bool = False
    default: Any = None
    help: str = ""


class FunctionIoBase(FrozenPydanticBase):
    schema_key: str
    _schema: Optional[Schema] = Field(None, alias="schema")

    def resolve(self, lib: ComponentLibrary) -> FunctionIoBase:
        d = self.dict()
        if self.schema_key:
            d["_schema"] = lib.get_schema(self.schema_key)
        return type(self)(**d)

    @property
    def is_generic(self) -> bool:
        return is_generic(self.schema_key)


class DataFunctionInputCfg(FunctionIoBase):
    name: str
    input_type: InputType = InputType.DataBlock
    required: bool = True
    description: Optional[str] = None

    @property
    def is_stream(self) -> bool:
        return self.input_type == InputType.Stream

    @property
    def is_self_reference(self) -> bool:
        return self.input_type == InputType.SelfReference

    @property
    def is_reference(self) -> bool:
        return self.input_type in (InputType.Reference, InputType.SelfReference)


class DataFunctionOutputCfg(FunctionIoBase):
    name: str = DEFAULT_OUTPUT_NAME
    data_format: Optional[str] = None
    # reference: bool = False # TODO: not a thing right? that's up to downstream to decide
    # optional: bool = False
    is_iterator: bool = False
    is_default: bool = True  # TODO: not here
    # stream: bool = False # TODO: do we ever need this?
    description: Optional[str] = None


class DataFunctionInterfaceCfg(FrozenPydanticBase):
    inputs: Dict[str, DataFunctionInputCfg]
    outputs: Dict[str, DataFunctionOutputCfg]
    parameters: Dict[str, Parameter]
    stdin: Optional[str] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    uses_context: bool = False
    uses_state: bool = False
    uses_error: bool = False
    none_output: bool = (
        False  # When return explicitly set to None (for identifying exporters) #TODO
    )

    def resolve(self, lib: ComponentLibrary) -> DataFunctionInterfaceCfg:
        d = self.dict()
        for n, i in self.inputs.items():
            d["inputs"][n] = i.resolve(lib)
        for n, i in self.outputs.items():
            d["outputs"][n] = i.resolve(lib)
        return DataFunctionInterfaceCfg(**d)

    def get_input(self, name: str) -> DataFunctionInputCfg:
        return self.inputs[name]

    def get_single_input(self) -> DataFunctionInputCfg:
        assert len(self.inputs) == 1, self.inputs
        return self.inputs[list(self.inputs)[0]]

    def get_single_non_reference_input(self) -> DataFunctionInputCfg:
        inpts = self.get_non_reference_inputs()
        assert len(inpts) == 1, inpts
        return inpts[list(inpts)[0]]

    def get_non_reference_inputs(self) -> Dict[str, DataFunctionInputCfg]:
        # TODO: should only ever be one of these
        return {n: i for n, i in self.inputs.items() if not i.is_reference}

    def get_stdin_name(self) -> Optional[str]:
        if self.stdin:
            return self.stdin
        try:
            nonref = self.get_single_non_reference_input()
            return nonref.name
        except AssertionError:
            pass
        inp = self.get_single_input()
        if inp:
            return inp.name
        return None

    # def assign_translations(
    #     self,
    #     declared_schema_translation: Optional[Dict[str, Union[Dict[str, str], str]]],
    # ) -> Optional[Dict[str, Dict[str, str]]]:
    #     if not declared_schema_translation:
    #         return None
    #     v = list(declared_schema_translation.values())[0]
    #     if isinstance(v, str):
    #         # Just one translation, so should be one input
    #         assert (
    #             len(self.get_non_recursive_inputs()) == 1
    #         ), "Wrong number of translations"
    #         return {
    #             self.get_single_non_recursive_input().name: declared_schema_translation
    #         }
    #     if isinstance(v, dict):
    #         return declared_schema_translation
    #     raise TypeError(declared_schema_translation)

    def get_default_output(self) -> Optional[DataFunctionOutputCfg]:
        return self.outputs.get(DEFAULT_OUTPUT_NAME)


class DataFunctionCfg(FrozenPydanticBase):
    name: str
    namespace: str
    interface: Optional[DataFunctionInterfaceCfg] = None
    required_storage_classes: List[str] = []
    required_storage_engines: List[str] = []
    ignore_signature: bool = (
        False  # Whether to ignore signature if there are any declared i/o
    )
    package_absolute_path: Optional[str] = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    _original_object: Any = None

    @property
    def key(self) -> str:
        assert self.name and self.namespace
        return f"{self.namespace}.{self.name}"

    def resolve(self, lib: ComponentLibrary) -> DataFunctionCfg:
        d = self.dict()
        if self.interface:
            d["interface"] = self.interface.resolve(lib)
        return DataFunctionCfg(**d)


class DataFunctionPackageCfg(FrozenPydanticBase):
    root_path: str
    function: DataFunctionCfg
    # local_vars: Dict = None
    # root_module: ModuleType
    tests: List[Dict] = []
    function_python_name: Optional[str] = None
    readme_name: str = "README.md"
    # python_requirements_path: str = None
    # docker_file_path: str = None
