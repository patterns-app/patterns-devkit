from __future__ import annotations

import os
from snapflow.templates.generator import generate_module, generate_schema, generate_snap
from cleo import Command


class GenerateCommand(Command):
    """
    Generate new

    new
        {type : Type of component to generate, one of (module, snap, schema)}
    """

    def handle(self):
        type_ = self.argument("type")
        name = self.ask(f"{type_} name", "")
        namespace = self.ask("namespace", "")  # TODO get somehow
        if type_ == "module":
            ctx = dict(module_name=name, namespace=namespace)
            generate_module(**ctx)
        elif type_ == "snap":
            os.chdir("snaps")
            ctx = dict(snap_name=name, namespace=namespace)
            generate_snap(**ctx)
        elif type_ == "schema":
            os.chdir("schemas")
            raise NotImplementedError
            ctx = dict(schema_name=name, namespace=namespace)
            generate_schema(**ctx)
        else:
            raise ValueError(f"Invalid type {type_}")
