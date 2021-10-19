from basis.cli.api import (
    env_list,
    env_logs,
    graph_list,
    graph_logs,
    node_list,
    node_logs,
)
from basis.cli.commands.base import BasisCommandBase
from cleo import Command


class ListCommand(BasisCommandBase, Command):
    """
    List all of given object type

    list
        {type : Type of object, one of [env, graph, node]}
    """

    def handle(self):
        self.ensure_authenticated()
        obj_type = self.argument("type")
        if obj_type == "env":
            resp = env_list()
        elif obj_type == "graph":
            resp = graph_list()
        elif obj_type == "node":
            resp = node_list()
        else:
            self.line(f"<error>Invalid type: {obj_type}</error>")
            exit(1)
        if not resp.ok:
            self.line(f"<error>Logs request failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        self.line(f"<info>{data}</info>")
