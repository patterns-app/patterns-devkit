from __future__ import annotations

import json

from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.graph.builder import graph_manifest_from_yaml


class ManifestCommand(BasisCommandBase, Command):
    """
    Builds manifest for given graph

    manifest
        {graph : Path to graph.yml file}
        {--python : Format as python dict, instead of json}
    """

    def handle(self):
        cfg_arg = self.argument("graph")
        python_opt = self.option("python")
        assert isinstance(cfg_arg, str)
        manifest = graph_manifest_from_yaml(cfg_arg)
        dct = manifest.dict(exclude_none=True)
        if python_opt:
            self.line(str(dct))
        else:
            manifest_json_str = json.dumps(dct)
            self.line(manifest_json_str)
