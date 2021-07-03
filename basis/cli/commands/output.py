from __future__ import annotations

import importlib
import os
from pathlib import Path

import yaml
from cleo import Command
from basis.cli.commands.base import BasisCommandBase
from basis.core.declarative.base import load_yaml
from basis.core.declarative.dataspace import DataspaceCfg
from basis.core.environment import Environment


class OutputCommand(BasisCommandBase, Command):
    """
    Get output from specified node.

    output
        {node : node key }
        {--f|format= : data format to output (defaults to csv). One of (csv, json) }
    """

    def handle(self):
        try:
            mod = self.import_current_basis_module()
            importlib.reload(mod)
        except AssertionError:
            pass
        node_key = self.argument("node")
        # config_file = self.option("config")
        # dataformat = self.option("format")
        # config_file = config_file or "basis.yml"
        config_file = "basis.yml"
        ds = DataspaceCfg(**load_yaml(config_file))
        block = Environment(dataspace=ds).get_latest_output(ds.graph.get_node(node_key))
        print(block.as_records())  # TODO
