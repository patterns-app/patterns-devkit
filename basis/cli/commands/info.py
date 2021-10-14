from basis.cli.api import app_info, node_info, project_info

from cleo import Command

from basis.cli.commands.base import BasisCommandBase


class InfoCommand(BasisCommandBase, Command):
    """
    Fetch basic information on given object

    info
        {type : Type of object, one of [project, app, node]}
        {name : Name of object}
    """

    def handle(self):
        self.ensure_authenticated()
        obj_type = self.argument("type")
        name = self.argument("type")
        params = {"name": name}
        if obj_type == "project":
            resp = project_info(params)
        elif obj_type == "app":
            resp = app_info(params)
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

