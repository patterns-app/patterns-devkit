from __future__ import annotations

import json
import pprint
from pathlib import Path

from basis.cli.commands.base import BasisCommandBase
from basis.cli.commands.upload import graph_cfg_from_argument
from basis.cli.templates.generator import generate_template
from basis.graph.builder import graph_as_configured_nodes
from cleo import Command


class ManifestCommand(BasisCommandBase, Command):
    """
    Builds manifest for given graph

    manifest
        {graph : Path to graph.yml file}
    """

    def handle(self):
        cfg_arg = self.argument("graph")
        assert isinstance(cfg_arg, str)
        graph_cfg = graph_cfg_from_argument(cfg_arg)
        cfg_path = Path(cfg_arg)
        cfg_dir = cfg_path.parent
        manifest = graph_as_configured_nodes(
            graph_cfg, abs_filepath_to_root=str(cfg_dir.resolve())
        )
        manifest_str = pprint.pformat(manifest.dict(exclude_unset=True))
        manifest_json_str = json.dumps(manifest.dict(exclude_unset=True))
        self.line(manifest_str)
        self.line(manifest_json_str)
