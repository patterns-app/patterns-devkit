from __future__ import annotations

import importlib
import os
from pathlib import Path

import yaml
from cleo import Command
from snapflow.cli.commands.base import SnapflowCommandBase
from snapflow.core.component import global_library
from snapflow.core.declarative.base import load_yaml
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.environment import Environment, run_graph, run_node
from snapflow.core.execution import execute_to_exhaustion
from snapflow.templates.generator import generate_template


class RunCommand(SnapflowCommandBase, Command):
    """
    Run a dataspace node. If no node specified, runs entire graph.

    run
        {node? : name of node to run (runs entire dataspace if not specified) }
        {--c|config= : path to dataspace configuration yaml }
        {--t|timelimit= : per-node execution time limit, in seconds }
        {--s|storage= : target storage url for execution output (e.g. postgres://localhost/snapflow) }
    """

    def handle(self):
        try:
            mod = self.import_current_snapflow_module()
            importlib.reload(mod)
        except AssertionError:
            pass
        node_key = self.argument("node")
        config_file = self.option("config")
        timelimit = self.option("timelimit")
        storage = self.option("storage")
        config_file = config_file or "snapflow.yml"
        ds = DataspaceCfg(**load_yaml(config_file))
        if node_key:
            run_node(
                node_key,
                graph=ds.graph,
                execution_timelimit_seconds=timelimit,
                target_storage=storage,
                env=Environment(dataspace=ds),
            )
        else:
            run_graph(
                ds.graph,
                execution_timelimit_seconds=timelimit,
                target_storage=storage,
                env=Environment(dataspace=ds),
            )
