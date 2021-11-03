import base64
import os
from pathlib import Path

from basis.cli.commands.base import BasisCommandBase
from basis.cli.config import get_current_organization_name
from basis.cli.helpers import compress_directory
from basis.cli.services.deploy import deploy_graph_version
from basis.cli.services.graph_versions import (
    list_graph_versions,
    get_latest_graph_version,
)
from basis.cli.services.upload import upload_graph_version
from basis.configuration.base import load_yaml
from cleo import Command

from basis.configuration.graph import NodeDefinitionCfg


class DeployCommand(BasisCommandBase, Command):
    """
    Deploy a graph to an environment on getbasis.com

    deploy
        {graph : Graph name}
        {environment : Environment name}
        {--g|graph-version : Specific graph version (default latest)}
    """

    def handle(self):
        self.ensure_authenticated()
        graph_name = self.argument("graph")
        env_name = self.argument("environment")
        graph_version = self.option("graph-version")
        assert not graph_version, "Graph version not supported yet"
        assert isinstance(graph_name, str)
        assert isinstance(env_name, str)
        org_name = get_current_organization_name()
        try:
            version = get_latest_graph_version(graph_name, org_name)
        except Exception as e:
            self.line(f"<error>Couldn't find graph version: {e}</error>")
            exit(1)
        try:
            data = deploy_graph_version(version["uid"], environment_name=env_name)
        except Exception as e:
            self.line(f"<error>Couldn't deploy graph: {e}</error>")
            exit(1)
        self.line(f"Graph deployed (<info>{data}</info>)")
