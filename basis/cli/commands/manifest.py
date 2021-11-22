from __future__ import annotations

import json
from pathlib import Path

from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.graph.builder import graph_manifest_from_yaml


class ManifestCommand(BasisCommandBase, Command):
    """
    Builds manifest for given graph

    manifest
        {graph : Path to graph.yml file}
    """

    def handle(self):
        cfg_arg = self.argument("graph")
        assert isinstance(cfg_arg, str)
        manifest = graph_manifest_from_yaml(cfg_arg)
        manifest_json_str = json.dumps(manifest.dict(exclude_none=True))
        self.line(manifest_json_str)
