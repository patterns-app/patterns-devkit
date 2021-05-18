from __future__ import annotations

import os
from pathlib import Path

import yaml
from cleo import Command
from snapflow.templates.generator import generate_template


class RunCommand(Command):
    """
    Run a dataspace or node

    run
        {dataspace : path to dataspace configuration yaml }
        {node? : name of node to run (runs entire dataspace if not specified) }
        {--t|timelimit : per-node execution time limit, in seconds }
        {--s|storage : target storage for execution output }
    """

    def handle(self):
        dataspace_file = self.argument("dataspace")
        node_key = self.argument("node")
        timelimit = self.option("timelimit")
        print(dataspace_file, node_key, timelimit)
