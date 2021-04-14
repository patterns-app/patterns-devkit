from __future__ import annotations

import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from commonmodel.base import Schema, SchemaLike
from dcp.data_format.handler import get_handler_for_name, infer_schema_for_name
from dcp.data_format.formats.memory.records import PythonRecordsHandler
from dcp.storage.base import Storage
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from dcp.utils.common import rand_str
from dcp.utils.data import read_csv, read_json, read_raw_string_csv
from pandas import DataFrame
from snapflow import DataBlock, Environment, Graph, _Snap
from snapflow.core.module import SnapflowModule
from snapflow.core.node import DataBlockLog, Node, SnapLog
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import select


def display_snap_log(env: Environment):
    for dbl in env.md_api.execute(
        select(DataBlockLog).order_by(DataBlockLog.created_at)
    ):
        print(f"{dbl.snap_log.snap_key:30} {dbl.data_block_id:4} {dbl.direction}")


def str_as_dataframe(
    env: Environment,
    test_data: str,
    module: Optional[SnapflowModule] = None,
    nominal_schema: Optional[Schema] = None,
) -> DataFrame:
    # TODO: add conform_dataframe_to_schema option
    if test_data.endswith(".csv"):
        if module is None:
            raise
        with module.open_module_file(test_data) as f:
            raw_records = list(read_csv(f.readlines()))
    elif test_data.endswith(".json"):
        if module is None:
            raise
        with module.open_module_file(test_data) as f:
            raw_records = [read_json(line) for line in f]
    else:
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
    module: Optional[SnapflowModule] = None

    def as_dataframe(self, env: Environment):
        schema = None
        if self.schema:
            schema = env.get_schema(self.schema)
        return str_as_dataframe(
            env, self.data, module=self.module, nominal_schema=schema
        )

    def get_schema_key(self) -> Optional[str]:
        if not self.schema:
            return None
        if isinstance(self.schema, str):
            return self.schema
        return self.schema.key


@contextmanager
def produce_snap_output_for_static_input(
    snap: _Snap,
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
    if target_storage:
        target_storage = env.add_storage(target_storage)
    with env.md_api.begin():
        g = Graph(env)
        input_datas = inputs
        input_nodes: Dict[str, Node] = {}
        pi = snap.get_interface()
        if not isinstance(inputs, dict):
            assert len(pi.get_non_recursive_inputs()) == 1
            input_datas = {pi.get_non_recursive_inputs()[0].name: inputs}
        for inpt in pi.inputs:
            if inpt.from_self:
                continue
            assert inpt.name is not None
            input_data = input_datas[inpt.name]
            if isinstance(input_data, str):
                input_data = DataInput(data=input_data)
            n = g.create_node(
                key=f"_input_{inpt.name}",
                snap="core.import_dataframe",
                params={
                    "dataframe": input_data.as_dataframe(env),
                    "schema": input_data.get_schema_key(),
                },
            )
            input_nodes[inpt.name] = n
        test_node = g.create_node(
            key=f"{snap.name}", snap=snap, params=params, inputs=input_nodes
        )
        blocks = env.produce(
            test_node, to_exhaustion=False, target_storage=target_storage
        )
        yield blocks
