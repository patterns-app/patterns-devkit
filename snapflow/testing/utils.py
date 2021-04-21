from __future__ import annotations

import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Union

from commonmodel.base import Schema, SchemaLike
from dcp.data_format.formats.memory.records import PythonRecordsHandler
from dcp.data_format.handler import get_handler_for_name, infer_schema_for_name
from dcp.storage.base import Storage
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from dcp.utils.common import rand_str
from dcp.utils.data import read_csv, read_json, read_raw_string_csv
from dcp.utils.pandas import assert_dataframes_are_almost_equal
from pandas import DataFrame
from snapflow import DataBlock, DataFunction, Environment, Graph
from snapflow.core.function import DEFAULT_OUTPUT_NAME
from snapflow.core.function_package import DataFunctionPackage
from snapflow.core.module import SnapflowModule
from snapflow.core.node import DataBlockLog, DataFunctionLog, Node
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import select


def display_function_log(env: Environment):
    for dbl in env.md_api.execute(
        select(DataBlockLog).order_by(DataBlockLog.created_at)
    ):
        print(
            f"{dbl.function_log.function_key:30} {dbl.data_block_id:4} {dbl.direction}"
        )


def str_as_dataframe(
    env: Environment,
    test_data: str,
    package: Optional[DataFunctionPackage] = None,
    nominal_schema: Optional[Schema] = None,
) -> DataFrame:
    # TODO: add conform_dataframe_to_schema option
    # TODO: support files
    # if test_data.endswith(".csv"):
    #     if module is None:
    #         raise
    #     with module.open_module_file(test_data) as f:
    #         raw_records = list(read_csv(f.readlines()))
    # elif test_data.endswith(".json"):
    #     if module is None:
    #         raise
    #     with module.open_module_file(test_data) as f:
    #         raw_records = [read_json(line) for line in f]
    # else:
    # Raw str csv
    raw_records = list(read_raw_string_csv(test_data))
    tmp = "_test_obj_" + rand_str()
    env._local_python_storage.get_api().put(tmp, raw_records)
    if nominal_schema is None:
        auto_schema = infer_schema_for_name(tmp, env._local_python_storage)
        nominal_schema = auto_schema
    else:
        PythonRecordsHandler().cast_to_schema(
            tmp, env._local_python_storage, nominal_schema
        )
    df = DataFrame.from_records(raw_records)
    return df


@dataclass
class DataInput:
    data: str
    schema: Optional[SchemaLike] = None
    package: Optional[DataFunctionPackage] = None

    def as_dataframe(self, env: Environment):
        schema = None
        if self.schema:
            schema = env.get_schema(self.schema)
        return str_as_dataframe(
            env, self.data, package=self.package, nominal_schema=schema
        )

    def get_schema_key(self) -> Optional[str]:
        if not self.schema:
            return None
        if isinstance(self.schema, str):
            return self.schema
        return self.schema.key

    @classmethod
    def from_input(
        cls, input: Union[str, Dict], package: DataFunctionPackage
    ) -> DataInput:
        data = None
        schema = None
        if isinstance(input, str):
            data = input
        elif isinstance(input, dict):
            data = input["data"]
            schema = input["schema"]
        else:
            raise TypeError(input)
        return DataInput(data=data, schema=schema, package=package)


@dataclass
class TestCase:
    name: str
    inputs: Dict[str, DataInput]
    outputs: Dict[str, DataInput]
    package: Optional[DataFunctionPackage] = None

    @classmethod
    def from_test(cls, test: Dict, package: DataFunctionPackage) -> TestCase:
        inputs = {}
        for name, i in test.get("inputs", {}).items():
            inputs[name] = DataInput.from_input(i, package)
        outputs = {}
        for name, i in test.get("outputs", {}).items():
            outputs[name] = DataInput.from_input(i, package)
        return TestCase(
            name=test.get("__name__"), inputs=inputs, outputs=outputs, package=package
        )


def run_test_case(case: TestCase, **kwargs):
    with produce_function_output_for_static_input(
        function=case.package.function, inputs=case.inputs, **kwargs
    ) as blocks:
        if not case.outputs:
            assert len(blocks) == 0
        else:
            assert len(blocks) == 1  # TODO: multiple output blocks
            output = blocks[0]
            expected = case.outputs[DEFAULT_OUTPUT_NAME]  # TODO: custom output name
            assert_dataframes_are_almost_equal(
                output.as_dataframe(),
                expected.as_dataframe(output.manager.env),
                schema=output.nominal_schema,
            )


class TestFeatureNotImplementedError(Exception):
    pass


@contextmanager
def produce_function_output_for_static_input(
    function: DataFunction,
    params: Dict[str, Any] = None,
    input: Any = None,
    inputs: Any = None,
    env: Optional[Environment] = None,
    module: Optional[SnapflowModule] = None,
    target_storage: Optional[Storage] = None,
    upstream: Any = None,  # TODO: DEPRECATED
) -> Iterator[List[DataBlock]]:
    inputs = input or inputs or upstream
    if env is None:
        db = get_tmp_sqlite_db_url()
        env = Environment(metadata_storage=db)
    if module:
        env.add_module(module)
    if not target_storage:
        if "database" in function.required_storage_classes:
            target_storage = Storage(get_tmp_sqlite_db_url())
        if function.required_storage_engines:
            raise TestFeatureNotImplementedError(
                "No testing support for required storage engines yet"
            )
    if target_storage:
        target_storage = env.add_storage(target_storage)
    with env.md_api.begin():
        g = Graph(env)
        input_datas = inputs
        input_nodes: Dict[str, Node] = {}
        pi = function.get_interface()
        if not isinstance(inputs, dict):
            assert len(pi.get_non_recursive_inputs()) == 1
            input_datas = {pi.get_single_non_recursive_input().name: inputs}
        for inpt in pi.inputs.values():
            if inpt.is_self_reference:
                continue
            assert inpt.name is not None
            input_data = input_datas[inpt.name]
            if isinstance(input_data, str):
                input_data = DataInput(data=input_data)
            assert isinstance(input_data, DataInput)
            n = g.create_node(
                key=f"_input_{inpt.name}",
                function="core.import_dataframe",
                params={
                    "dataframe": input_data.as_dataframe(env),
                    "schema": input_data.get_schema_key(),
                },
            )
            input_nodes[inpt.name] = n
        test_node = g.create_node(
            key=f"{function.name}", function=function, params=params, inputs=input_nodes
        )
        blocks = env.produce(
            test_node, to_exhaustion=False, target_storage=target_storage
        )
        yield blocks
