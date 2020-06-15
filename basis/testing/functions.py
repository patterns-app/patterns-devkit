import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from pandas import DataFrame
from sqlalchemy.orm import close_all_sessions

from basis.core.data_function import DataFunctionLike
from basis.core.environment import Environment
from basis.core.streams import InputBlocks
from basis.core.typing.inference import infer_otype_from_records_list
from basis.core.typing.object_type import ObjectTypeLike
from basis.db.api import dispose_all
from basis.utils.common import cf, printd, rand_str
from basis.utils.data import read_csv
from basis.utils.pandas import (
    assert_dataframes_are_almost_equal,
    records_list_to_dataframe,
)


@dataclass(frozen=True)
class TestDataBlock:
    otype_like: ObjectTypeLike
    data_frame: DataFrame
    data_raw: Optional[str] = None


class DataFunctionTestCase:
    def __init__(
        self,
        name: str,
        function: Union[DataFunctionLike, str],
        test_datas: List[Dict[str, TestDataBlock]],
    ):
        self.name = name
        self.function = function
        self.test_datas = test_datas

    def as_input_blocks(self, env: Environment) -> InputBlocks:
        raise


class TestCase:
    def __init__(
        self,
        function: Union[DataFunctionLike, str],
        tests: Dict[str, List[Dict[str, str]]],
    ):
        self.function = function
        self.tests = self.process_raw_tests(tests)

    def process_raw_tests(self, tests) -> List[DataFunctionTestCase]:
        cases = []
        for test_name, raw_test_datas in tests.items():
            test_datas = []
            for test_data in raw_test_datas:
                test_data_blocks = {}
                for input_name, data in test_data.items():
                    if data:
                        df, otype = self.process_raw_test_data(data)
                    else:
                        otype = None
                        df = None
                    test_data_blocks[input_name] = TestDataBlock(
                        otype_like=otype, data_frame=df, data_raw=data
                    )
                test_datas.append(test_data_blocks)
            case = DataFunctionTestCase(
                name=test_name, function=self.function, test_datas=test_datas,
            )
            cases.append(case)
        return cases

    def process_raw_test_data(self, test_data: str) -> Tuple[DataFrame, ObjectTypeLike]:
        lines = [l.strip() for l in test_data.split("\n") if l.strip()]
        assert lines, "Empty test data"
        otype = None
        if lines[0].startswith("otype:"):
            otype = lines[0][6:].strip()
            lines = lines[1:]
        records = read_csv(lines)
        auto_otype = infer_otype_from_records_list(records)
        df = records_list_to_dataframe(records, auto_otype)
        return df, otype

    @contextmanager
    def test_env(self, **kwargs: Any) -> Environment:
        # TODO: need way more hooks here (adding runtimes and storages, for instance)
        from basis.core.environment import Environment
        from basis.db.api import create_db, drop_db

        # TODO: what is this hack
        db_name = f"__test_{rand_str(10).lower()}"
        conn_url = f"postgres://postgres@localhost:5432/postgres"
        db_url = f"postgres://postgres@localhost:5432/{db_name}"
        # conn_url = f"sqlite:///" + db_name + ".db"
        # try:
        #     drop_db(conn_url, db_name)
        # except Exception as e:
        #     pass
        create_db(conn_url, db_name)
        env = Environment(db_name, metadata_storage=db_url, **kwargs)
        env.add_storage(db_url)
        try:
            yield env
        finally:
            close_all_sessions()
            dispose_all()
            drop_db(conn_url, db_name)

    def run(self, **env_args: Any):
        # TODO: clean this function up
        for case in self.tests:
            print(f"{case.name}:")
            with self.test_env(**env_args) as env:
                fn = env.get_function(self.function)
                dfi = fn.get_interface()
                test_node = env.add_node("_test_node", fn)
                for i, test_data in enumerate(case.test_datas):
                    try:
                        inputs = {}
                        for input in dfi.inputs:
                            test_df = test_data[input.name].data_frame
                            test_otype = test_data[input.name].otype_like
                            n = env.add_external_source_node(
                                f"_test_source_node_{input.name}_{i}",
                                "DataFrameExternalResource",
                                config={"dataframe": test_df, "otype": test_otype},
                            )
                            inputs[input.name] = n
                        test_node.set_upstream(inputs)
                        output = env.produce(test_node, to_exhaustion=False)
                        if "output" in test_data:
                            output_df = output.as_dataframe()
                            expected_df = test_data["output"].data_frame
                            expected_otype = env.get_otype(
                                test_data["output"].otype_like
                            )
                            printd("Output", output_df)
                            printd("Expected", expected_df)
                            assert_dataframes_are_almost_equal(
                                output_df, expected_df, expected_otype
                            )
                        else:
                            assert output is None, f"Unexpected output {output}"
                        print(cf.success("Ok"))
                    except Exception as e:
                        print(cf.error("Fail:"), str(e))
                        raise e


DataFunctionTestCaseLike = Union[DataFunctionTestCase, str]


# def test_cases_from_yaml(yml: str, module: BasisModule) -> List[DataFunctionTestCase]:
#     d = strictyaml.load(yml).data
#     fn = d.get("function")
#     fn = module.get_function(fn)
#     tests = d.get("tests")
#     cases = []
#     for test_name, test_inputs in tests.items():
#         test_data = {}
#         for input_name, data in test_inputs.items():
#             test_data[input_name] = pd.read_csv(StringIO(data.strip()))
#         DataFunctionTestCase(
#             name=test_name, function=fn, test_data=test_data,
#         )
#     return cases
