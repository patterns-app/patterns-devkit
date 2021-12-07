from cleo import Command

from basis.cli.commands.base import BasisCommandBase
from basis.cli.services.logs import get_execution_logs
from basis.cli.config import read_local_basis_config


class LogsCommand(BasisCommandBase, Command):
    """
    list execution logs for a given graph

    logs
        {graph : Graph name}
        {environment : Environment name}
        {absolute-node-path? : Absolute path of node}
    """

    def handle(self):
        self.ensure_authenticated()
        graph_name = self.argument("graph")
        environment_name = self.argument("environment")
        node_path = self.argument("absolute-node-path")

        try:
            cfg = read_local_basis_config()
            events = get_execution_logs(
                cfg.organization_name, environment_name, graph_name, node_path
            )
        except Exception as e:
            self.line(f"<error>Error reading logs: {e}</error>")
            exit(1)
        if not events:
            self.line(f"No events found")
        else:
            table = self.table()
            table.set_header_row(list(events[0].keys()))
            for e in events:
                # cleo can't properly display tables with large cells, so truncate error tracebacks if there are any
                if "error" in e:
                    e["error"] = e["error"][:80] + ("…" if len(e["error"]) > 80 else "")
                table.add_row(list(str(v) for v in e.values()))
            table.render(self.io)
