from basis.cli.commands.base import BasisCommandBase
from cleo import Command


class RunCommand(BasisCommandBase, Command):
    """
    Run a component or node. If component, then local component file is uploaded and run against given graph as temporary version.

    run
        {type : Type of object, one of [component, node]}
        {name-or-path : Name of node or path to component}
        {--graph : Graph}
    """

    def handle(self):
        self.ensure_authenticated()
        obj_type = self.argument("type")
        name = self.argument("name-or-path")
        ds = self.option("graph")
        params = {"name": name}
        if ds:
            params["graph"] = ds
        if obj_type == "node":
            resp = run_node(params)
        elif obj_type == "component":
            raise NotImplementedError
            # TODO: upload current graph
            # And then run node
            resp = run_node(params)
        else:
            self.line(f"<error>Invalid type: {obj_type}</error>")
            exit(1)
        if not resp.ok:
            self.line(f"<error>Run failed: {resp.text}</error>")
            exit(1)
        data = resp.json()
        self.line(f"<info>{data}</info>")
