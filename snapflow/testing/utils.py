from __future__ import annotations

import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Union

from commonmodel.base import Schema, SchemaLike
from dcp.data_format.formats.memory.records import PythonRecordsHandler
from dcp.data_format.handler import get_handler_for_name, infer_schema_for_name
from dcp.storage.base import Storage, get_engine_for_scheme
from dcp.storage.database.api import DatabaseApi, DatabaseStorageApi
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from dcp.utils.common import rand_str
from dcp.utils.data import read_csv, read_json, read_raw_string_csv
from dcp.utils.pandas import assert_dataframes_are_almost_equal
from loguru import logger
from pandas import DataFrame
from snapflow import DataBlock, DataFunction, Environment
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.function import DEFAULT_OUTPUT_NAME
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.function_package import DataFunctionPackage
from snapflow.core.module import SnapflowModule
from snapflow.core.state import DataBlockLog
from sqlalchemy.exc import OperationalError
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
    def from_test(cls, name: str, test: Dict, package: DataFunctionPackage) -> TestCase:
        inputs = {}
        for input_name, i in test.get("inputs", {}).items():
            inputs[input_name] = DataInput.from_input(i, package)
        outputs = {}
        for output_name, i in test.get("outputs", {}).items():
            outputs[output_name] = DataInput.from_input(i, package)
        return TestCase(name=name, inputs=inputs, outputs=outputs, package=package)


def run_test_case(case: TestCase, **kwargs):
    # logger.enable("dcp")
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
    # logger.disable("dcp")


class TestFeatureNotImplementedError(Exception):
    pass


@contextmanager
def provide_test_storages(
    function: DataFunction, target_storage: Storage
) -> Iterator[Optional[Storage]]:
    if target_storage:
        yield target_storage  # TODO
    elif function.required_storage_engines:
        # TODO: multiple engines -- is it AND or OR?? each entry is AND and inside entry commas delim OR
        eng = get_engine_for_scheme(function.required_storage_engines[0])
        api_cls = eng.get_api_cls()
        if issubclass(api_cls, DatabaseApi):
            if not api_cls.dialect_is_supported():
                raise TestFeatureNotImplementedError(eng)
            try:
                with api_cls.temp_local_database() as url:
                    yield Storage(url)
            except OperationalError:  # TODO: might catch a lot of real errors?
                raise TestFeatureNotImplementedError(eng)

    elif "database" in function.required_storage_classes:
        yield Storage(get_tmp_sqlite_db_url())
    else:
        yield None


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

    with provide_test_storages(function, target_storage) as target_storage:
        if env is None:
            db = get_tmp_sqlite_db_url()
            ds = DataspaceCfg(
                metadata_storage=db,
                storages=[target_storage.url] if target_storage else [],
            )
            env = Environment(dataspace=ds)
        if module:
            env.add_module(module)
        with env.md_api.begin():
            input_datas = inputs
            input_nodes: Dict[str, GraphCfg] = {}
            pi = function.get_interface()
            if not isinstance(inputs, dict):
                # assert len(pi.get_non_reference_inputs()) == 1
                input_datas = {pi.get_single_input().name: inputs}
            for inpt in pi.inputs.values():
                if inpt.is_self_reference:
                    continue
                assert inpt.name is not None
                input_data = input_datas[inpt.name]
                if isinstance(input_data, str):
                    input_data = DataInput(data=input_data)
                assert isinstance(input_data, DataInput)
                n = GraphCfg(
                    key=f"_input_{inpt.name}",
                    function="core.import_dataframe",
                    params={
                        "dataframe": input_data.as_dataframe(env),
                        "schema": input_data.get_schema_key(),
                    },
                )
                input_nodes[inpt.name] = n
            test_node = GraphCfg(
                key=f"{function.name}",
                function=function.key,
                params=params or {},
                inputs={k: i.key for k, i in input_nodes.items()},
            )
            graph = GraphCfg(nodes=[test_node] + list(input_nodes.values()))
            blocks = env.produce(
                node=test_node,
                graph=graph,
                to_exhaustion=False,
                target_storage=target_storage,
            )
            yield blocks
