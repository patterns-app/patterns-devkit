import csv
from io import StringIO
from typing import Dict, List, Tuple, Union

import pandas as pd
import strictyaml
from pandas import DataFrame

from basis.core.data_function import DataFunction, DataFunctionLike, ensure_datafunction
from basis.core.environment import Environment
from basis.core.module import BasisModule
from basis.core.streams import InputBlocks
from basis.core.typing.inference import infer_otype_from_records_list
from basis.core.typing.object_type import ObjectTypeLike
from basis.utils.data import read_csv
from basis.utils.pandas import records_list_to_dataframe


class DataFunctionTestCase:
    def __init__(
        self,
        name: str,
        function: Union[DataFunctionLike, str],
        test_data: Dict[str, DataFrame],
        test_data_otypes: Dict[str, ObjectTypeLike] = None,
    ):
        self.name = name
        self.function = function
        self.test_data = test_data
        self.test_data_otypes = test_data_otypes

    def as_input_blocks(self, env: Environment) -> InputBlocks:
        raise


class TestCase:
    def __init__(
        self, function: Union[DataFunctionLike, str], tests: Dict[str, Dict[str, str]],
    ):
        self.function = function
        self.tests = self.process_raw_tests(tests)

    def process_raw_tests(self, tests) -> List[DataFunctionTestCase]:
        cases = []
        for test_name, test_inputs in tests.items():
            test_data = {}
            test_data_otypes = {}
            for input_name, data in test_inputs.items():
                if data:
                    df, otype = self.process_raw_test_data(data)
                else:
                    otype = None
                    df = None
                test_data[input_name] = df
                test_data_otypes[input_name] = otype
            case = DataFunctionTestCase(
                name=test_name,
                function=self.function,
                test_data=test_data,
                test_data_otypes=test_data_otypes,
            )
            cases.append(case)
        return cases

    def process_raw_test_data(self, test_data: str) -> Tuple[DataFrame, ObjectTypeLike]:
        lines = [l.strip() for l in test_data.split("\n") if l.strip()]
        assert lines
        otype = None
        if lines[0].startswith("otype:"):
            otype = lines[0][6:].strip()
            lines = lines[1:]
        records = read_csv(lines)
        auto_otype = infer_otype_from_records_list(records)
        df = records_list_to_dataframe(records, auto_otype)
        return df, otype


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
