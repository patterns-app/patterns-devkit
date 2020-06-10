from io import StringIO
from typing import Dict, List, Union

import pandas as pd
import strictyaml
from pandas import DataFrame

from basis.core.data_function import DataFunction, DataFunctionLike, ensure_datafunction
from basis.core.environment import Environment
from basis.core.module import BasisModule
from basis.core.streams import InputBlocks


class DataFunctionTestCase:
    def __init__(
        self,
        name: str,
        function: Union[DataFunctionLike, str],
        test_data: Dict[str, DataFrame],
    ):
        self.name = name
        self.function = function
        self.test_data = test_data

    def as_input_blocks(self, env: Environment) -> InputBlocks:
        raise


class TestCase:
    def __init__(
        self, function: Union[DataFunctionLike, str], tests: Dict[str, Dict[str, str]]
    ):
        self.function = function
        self.tests = self.process_raw_tests(tests)

    def process_raw_tests(self, tests) -> List[DataFunctionTestCase]:
        cases = []
        for test_name, test_inputs in tests.items():
            test_data = {}
            for input_name, data in test_inputs.items():
                if data:
                    d = "\n".join(l.strip() for l in data.split("\n") if l.strip())
                    input = pd.read_csv(StringIO(d.strip()))
                else:
                    input = None
                test_data[input_name] = input
            case = DataFunctionTestCase(
                name=test_name, function=self.function, test_data=test_data
            )
            cases.append(case)
        return cases


DataFunctionTestCaseLike = Union[DataFunctionTestCase, str]


def test_cases_from_yaml(yml: str, module: BasisModule) -> List[DataFunctionTestCase]:
    d = strictyaml.load(yml).data
    fn = d.get("function")
    fn = module.get_function(fn)
    tests = d.get("tests")
    cases = []
    for test_name, test_inputs in tests.items():
        test_data = {}
        for input_name, data in test_inputs.items():
            print(data.strip())
            test_data[input_name] = pd.read_csv(StringIO(data.strip()))
        DataFunctionTestCase(
            name=test_name, function=fn, test_data=test_data,
        )
    return cases
