from basis.cli.api import env_logs, graph_logs, node_logs
from basis.cli.commands.base import BasisCommandBase
from cleo import Command


class LogsCommand(BasisCommandBase, Command):
    """
    Fetch latest logs for object

    logs
        {type : Type of object, one of [env, graph, node]}
        {name : Name of object}
    """

    def handle(self):
        self.ensure_authenticated()
        obj_type = self.argument("type")
        name = self.argument("type")
        params = {"name": name}
        if obj_type == "env":
            resp = env_logs(params)
        elif obj_type == "graph":
            resp = graph_logs(params)
        elif obj_type == "node":
            resp = node_logs(params)
        else:
            self.line(f"<error>Invalid type: {obj_type}</error>")
            exit(1)
        if not resp.ok:
            self.line(f"<error>Logs request failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        self.line(f"<info>{data}</info>")
