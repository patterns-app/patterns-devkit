import os
from collections import OrderedDict
from pathlib import Path

import pytest
import requests_mock
from basis.cli.config import BASIS_CONFIG_ENV_VAR, read_local_basis_config
from basis.cli.services.api import API_BASE_URL
from basis.configuration.graph import GraphCfg
from basis.configuration.node import GraphNodeCfg
from pydantic import ValidationError
from tests.cli.base import IS_CI, get_test_command, set_tmp_dir


def test_graph_configuration():
    assert GraphCfg(
        name="mygraph",
        basis={"version": "0.1"},
        interface={
            "inputs": [{"name": "myinput"}],
            "outputs": OrderedDict(myoutput="suboutput"),
            "parameters": {"myparam": 1},
        },
        nodes=[
            {"python": "test.py"},
            {"sql": "test.sql"},
        ],
    )
