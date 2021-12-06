from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.config import get_current_organization_name
from basis.cli.services.graph_versions import get_latest_graph_version
from basis.cli.services.run import trigger_node


class RunCommand(BasisCommandBase, Command):
    """
    Run a node in a deployed graph.

    run
        {name : Name of node to run}
        {graph : Graph name}
        {environment : Environment name}
        {--local : Execute node locally}
    """

    def handle(self):
        self.ensure_authenticated()
        node_name = self.argument("name")
        graph_name = self.argument("graph")
        env_name = self.argument("environment")
        local_exec = self.option("local")
        assert isinstance(graph_name, str)
        assert isinstance(env_name, str)
        assert isinstance(node_name, str)
        org_name = get_current_organization_name()
        try:
            # TODO: get latest deployed version
            version = get_latest_graph_version(graph_name, org_name)
        except Exception as e:
            self.line(f"<error>Couldn't find graph version: {e}</error>")
            exit(1)
        try:
            # manifest = version["manifest"]
            data = trigger_node(
                node_id=node_name,  # TODO
                graph_version_uid=version["uid"],
                environment_name=env_name,
                local_execution=bool(local_exec),
            )
        except Exception as e:
            self.line(f"<error>Couldn't run node: {e}</error>")
            exit(1)
        self.line(f"Node triggered to run")
