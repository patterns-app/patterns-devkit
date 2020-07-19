from __future__ import annotations

import pandas as pd
import pytest
from pandas._testing import assert_almost_equal

from basis.core.environment import Environment
from basis.modules import core


def test_example():
    env = Environment(metadata_storage="sqlite://")
    env.add_storage("memory://test")
    env.add_module(core)
    df = pd.DataFrame({"a": range(10), "b": range(10)})
    env.add_node("n1", "extract_dataframe", config={"dataframe": df})
    output = env.produce("n1")
    assert_almost_equal(output.as_dataframe(), df)
