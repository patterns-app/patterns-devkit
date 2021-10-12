from basis.cli.api import app_logs, node_logs, project_logs

from cleo import Command

from basis.cli.commands.base import BasisCommandBase


class LogsCommand(BasisCommandBase, Command):
    """
    Fetch latest logs for object

    logs
        {type : Type of object, one of [project, app, node]}
        {name : Name of object}
    """

    def handle(self):
        obj_type = self.argument("type")
        name = self.argument("type")
        params = {"name": name}
        if obj_type == "project":
            resp = project_logs(params)
        elif obj_type == "app":
            resp = app_logs(params)
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

