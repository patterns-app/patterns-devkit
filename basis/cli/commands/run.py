from __future__ import annotations

import importlib
import os
from pathlib import Path

import yaml
from basis.cli.commands.base import BasisCommandBase
from basis.core.configuration.base import load_yaml
from basis.core.configuration.environment import EnvironmentCfg, ProjectCfg
from basis.core.environment import Environment, run_graph, run_node
from basis.core.instantiation import instantiate_graph
from basis.core.library import global_library
from cleo import Command


class RunCommand(BasisCommandBase, Command):
    """
    Run a node. If no node specified, runs entire graph.

    run
        {node? : name of node to run (runs entire environment if not specified) }
        {--c|config= : path to environment configuration yaml }
        {--t|timelimit= : per-node execution time limit, in seconds }
        {--s|storage= : target storage url for execution output (e.g. postgres://localhost/basis) }
    """

    def handle(self):
        try:
            mod = self.import_current_basis_module()
            importlib.reload(mod)
        except AssertionError:
            pass
        node_key = self.argument("node")
        config_file = self.option("config")
        timelimit = self.option("timelimit")
        storage = self.option("storage")
        config_file = config_file or "basis.yml"
        pj = ProjectCfg(**load_yaml(config_file))
        env = Environment(cfg=pj.as_environment_cfg())
        graph = instantiate_graph(pj.nodes, env.library)
        if node_key:
            env.run_node(
                node_key,
                graph=graph,
                execution_timelimit_seconds=timelimit,
                target_storage=storage,
            )
        else:
            env.run_graph(
                graph=graph,
                execution_timelimit_seconds=timelimit,
                target_storage=storage,
            )
