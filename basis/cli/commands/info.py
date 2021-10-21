from basis.cli.services.api import env_info, graph_info, node_info
from basis.cli.commands.base import BasisCommandBase
from cleo import Command


class InfoCommand(BasisCommandBase, Command):
    """
    Fetch basic information on given object

    info
        {type : Type of object, one of [env, graph, node]}
        {name : Name of object}
    """

    def handle(self):
        self.ensure_authenticated()
        obj_type = self.argument("type")
        name = self.argument("type")
        params = {"name": name}
        if obj_type == "env":
            resp = env_info(params)
        elif obj_type == "graph":
            resp = graph_info(params)
        elif obj_type == "node":
            resp = node_info(params)
        else:
            self.line(f"<error>Invalid type: {obj_type}</error>")
            exit(1)
        if not resp.ok:
            self.line(f"<error>Info request failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        self.line(f"<info>{data}</info>")
