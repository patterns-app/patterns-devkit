import os
from pathlib import Path
from collections import OrderedDict

import pytest
import requests_mock
from basis.cli.api import DEFAULT_BASE_URL
from basis.cli.config import BASIS_CONFIG_ENV_VAR, read_local_basis_config
from basis.configuration.graph import GraphCfg
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir
from pydantic import ValidationError

from basis.configuration.node import NodeCfg


def test_graph_configuration():
    assert GraphCfg(
        name="mygraph",
        basis={"version": "0.1"},
        interface={
            "inputs": [{"name": "myinput"}],
            "outputs": OrderedDict(myoutput="suboutput"),
            "parameters": {"myparam": 1},
        },
        nodes=[{"python": "test.py"}, {"sql": "test.sql"},],
    )
