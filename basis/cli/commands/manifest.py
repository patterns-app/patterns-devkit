from __future__ import annotations

from pathlib import Path
import pprint

from basis.cli.commands.base import BasisCommandBase
from basis.cli.commands.upload import graph_cfg_from_argument
from basis.cli.templates.generator import generate_template
from cleo import Command

from basis.graph.builder import ConfiguredGraphBuilder


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
        manifest = ConfiguredGraphBuilder(
            directory=cfg_dir.absolute(), cfg=graph_cfg
        ).build_manifest_from_config()
        manifest_str = pprint.pformat(manifest.dict(exclude_unset=True))
        self.line(manifest_str)
