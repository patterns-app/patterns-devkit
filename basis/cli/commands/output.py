from __future__ import annotations

import importlib
import os
from pathlib import Path

import yaml
from basis.cli.commands.base import BasisCommandBase
from basis.core.configuration.base import load_yaml
from basis.core.configuration.environment import EnvironmentCfg, ProjectCfg
from basis.core.environment import Environment
from cleo import Command


class OutputCommand(BasisCommandBase, Command):
    """
    Get output from specified node.

    output
        {node : node key }
        {--c|config= : path to environment configuration yaml }
        {--f|format= : data format to output (defaults to csv). One of (csv, json) }
    """

    def handle(self):
        try:
            mod = self.import_current_basis_module()
            importlib.reload(mod)
        except AssertionError:
            pass
        node_key = self.argument("node")
        config_file = self.option("config")
        # dataformat = self.option("format")
        config_file = config_file or "basis.yml"
        pj = ProjectCfg(**load_yaml(config_file))
        env = Environment(cfg=pj.as_environment_cfg())
        block = env.get_latest_output(node_key)
        if block is not None:
            print(block.as_records())  # TODO
        else:
            print(f"No output for node")
