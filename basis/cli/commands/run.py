from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.config import get_current_organization_name
from basis.cli.services.graph_versions import get_latest_graph_version
from basis.cli.services.run import trigger_node


class RunCommand(BasisCommandBase, Command):
    """
    Run a node in a deployed graph.

    run
        {absolute-node-path : Absolute path of node}
        {graph : Graph name}
        {environment : Environment name}
    """

    def handle(self):
        self.ensure_authenticated()
        absolute_node_path = self.argument("absolute-node-path")
        graph_name = self.argument("graph")
        env_name = self.argument("environment")
        assert isinstance(graph_name, str)
        assert isinstance(env_name, str)
        assert isinstance(absolute_node_path, str)
        org_name = get_current_organization_name()
        try:
            # TODO: get latest deployed version
            version = get_latest_graph_version(graph_name, org_name)
        except Exception as e:
            self.line(f"<error>Couldn't find graph version: {e}</error>")
            exit(1)
        try:
            data = trigger_node(
                absolute_node_path=absolute_node_path,
                graph_version_uid=version["uid"],
                environment_name=env_name,
            )
        except Exception as e:
            self.line(f"<error>Couldn't run node: {e}</error>")
            exit(1)
        self.line(f"Node triggered to run")
