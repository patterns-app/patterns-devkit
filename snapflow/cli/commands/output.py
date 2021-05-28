from __future__ import annotations

import importlib
import os
from pathlib import Path

import yaml
from cleo import Command
from snapflow.cli.commands.base import SnapflowCommandBase
from snapflow.core.declarative.base import load_yaml
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.environment import Environment, run_graph, run_node
from snapflow.core.execution import execute_to_exhaustion
from snapflow.templates.generator import generate_template


class OutputCommand(SnapflowCommandBase, Command):
    """
    Get output from specified node.

    output
        {node : node key }
        {--f|format= : data format to output (defaults to csv). One of (csv, json) }
    """

    def handle(self):
        try:
            mod = self.import_current_snapflow_module()
            importlib.reload(mod)
        except AssertionError:
            pass
        node_key = self.argument("node")
        # config_file = self.option("config")
        # dataformat = self.option("format")
        # config_file = config_file or "snapflow.yml"
        config_file = "snapflow.yml"
        ds = DataspaceCfg(**load_yaml(config_file))
        block = Environment(dataspace=ds).get_latest_output(ds.graph.get_node(node_key))
        print(block.as_records())  # TODO
