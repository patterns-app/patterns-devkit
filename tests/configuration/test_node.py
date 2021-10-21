import os
from pathlib import Path

import pytest
import requests_mock
from basis.cli.services.api import API_BASE_URL
from basis.cli.config import BASIS_CONFIG_ENV_VAR, read_local_basis_config
from basis.configuration.node import GraphNodeCfg
from pydantic import ValidationError
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_node_configuration_one_type():
    assert GraphNodeCfg(python="test.py")
    assert GraphNodeCfg(sql="test.sql")
    assert GraphNodeCfg(subgraph="subgraph")
    with pytest.raises(ValidationError):
        GraphNodeCfg()
    with pytest.raises(ValidationError):
        GraphNodeCfg(python="test.py", sql="test.sql")


def test_node_configuration_full():
    assert GraphNodeCfg(
        python="test.py",
        name="override",
        parameters={"myparam": 1},
        inputs={"myinput": "othernode"},
        output_configurations={"myoutput": {"name": "diff_name"}},
    )
