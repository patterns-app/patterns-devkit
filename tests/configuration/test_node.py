import os
from pathlib import Path

import pytest
import requests_mock
from basis.cli.api import DEFAULT_BASE_URL
from basis.cli.config import BASIS_CONFIG_ENV_VAR, read_local_basis_config
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir
from pydantic import ValidationError

from basis.configuration.node import NodeCfg


def test_node_configuration_one_type():
    assert NodeCfg(python="test.py")
    assert NodeCfg(sql="test.sql")
    assert NodeCfg(subgraph="subgraph")
    with pytest.raises(ValidationError):
        NodeCfg()
    with pytest.raises(ValidationError):
        NodeCfg(python="test.py", sql="test.sql")


def test_node_configuration_full():
    assert NodeCfg(
        python="test.py",
        name="override",
        parameters={"myparam": 1},
        inputs={"myinput": "othernode"},
        output_configurations={"myoutput": {"name": "diff_name"}},
    )

