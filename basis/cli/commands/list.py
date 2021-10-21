from basis.cli.config import get_current_organization_uid
from basis.cli.services.list import list_objects
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
        assert isinstance(obj_type, str)
        try:
            objects = list_objects(obj_type, get_current_organization_uid())
        except Exception as e:
            self.line(f"<error>Error listing objects: {e}</error>")
            exit(1)
        if not objects:
            self.line(f"No {obj_type}s found")
        else:
            table = self.table()
            keys = list(objects[0].keys())
            table.set_header_row(keys)
            table.set_rows([list(d.values()) for d in objects])
            table.render(self.io)
