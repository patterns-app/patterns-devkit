from basis.cli.api import app_logs, dataspace_logs, node_logs
from basis.cli.commands.base import BasisCommandBase
from cleo import Command


class LogsCommand(BasisCommandBase, Command):
    """
    Fetch latest logs for object

    logs
        {type : Type of object, one of [dataspace, app, node]}
        {name : Name of object}
    """

    def handle(self):
        self.ensure_authenticated()
        obj_type = self.argument("type")
        name = self.argument("type")
        params = {"name": name}
        if obj_type == "dataspace":
            resp = dataspace_logs(params)
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
