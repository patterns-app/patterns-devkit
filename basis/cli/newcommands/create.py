import os
from pathlib import Path

import click
import typer
from typer import Option, Argument

from basis.cli.config import (
    read_local_basis_config,
    resolve_graph_path,
    write_local_basis_config,
)
from basis.cli.newapp import app
from basis.cli.services.output import abort, prompt_path
from basis.cli.services.output import print
from basis.configuration.base import dump_yaml, load_yaml

create = typer.Typer()

app.add_typer(create, name="create", help="Create a graph new or node")

_name_help = "The name of the graph. The location will be used as a name by default"


@create.command()
def graph(
    name: str = Option("", "--name", "-n", help=_name_help),
    location: Path = Argument(None),
):
    """Add a new node to a graph"""
    if not location:
        prompt = "Enter a location for the graph"
        location = prompt_path(prompt, exists=False)

    cfg = read_local_basis_config()
    path = resolve_graph_path(location, exists=False)
    name = name or location.stem
    path.write_text(dump_yaml({"name": name}))
    cfg.default_graph = path
    write_local_basis_config(cfg)

    print(f"\n[success]Created graph [b]{name}")
    print(
        f"\n[info]You can add nodes with [code]cd {location}[/code], then [code]basis create node[/code]"
    )


_graph_help = "The graph to add this node to"
_name_help = "The name of the node. The location will be used as a name by default"


@create.command()
def node(
    explicit_graph: Path = Option(None, "--graph", "-g", exists=True, help=_graph_help),
    name: str = Option("", "--name", "-n", help=_name_help),
    location: Path = Argument(None),
):
    """Add a new node to a graph

    basis create node --name='My Node' mynode.py
    """
    if not location:
        message = "Enter a location for the node [prompt.default](e.g. mynode.sql)"
        location = prompt_path(message, exists=False)

    if location.exists():
        abort(f"{location} already exists")

    cfg = read_local_basis_config()
    graph_path = resolve_graph_path(explicit_graph or cfg.default_graph, exists=True)
    graph_dir = graph_path.parent
    if not location.is_absolute() and not Path(os.getcwd()).resolve().is_relative_to(
        graph_dir
    ):
        print(
            f"[error]Cannot use a relative node location outside of the graph directory."
        )
        print(
            f"[info]Try changing your directory to the graph directory [code]({graph_dir})"
        )
        print(
            f"[info]You can change the graph directory for this command with the --graph option, or you can change the "
            f"default graph with 'basis config --graph'"
        )
        raise typer.Exit(1)

    # Create the node file
    if location.suffix == ".py":
        content = _PY_FILE_TEMPLATE.format(location.stem)
    elif location.suffix == ".sql":
        content = _SQL_FILE_TEMPLATE
    else:
        abort("Node file location must end in .py or .sql")

    # Update the graph yaml
    graph_dict = load_yaml(graph_path)
    nodes = graph_dict.get("nodes", [])
    node_file = "/".join(location.absolute().relative_to(graph_dir).parts)

    if any(n["node_file"] == node_file for n in nodes):
        abort(f"Node file {location} is already defined in the graph configuration")

    nodes.append({"name": name or location.stem, "node_file": node_file})
    graph_dict["nodes"] = nodes
    yaml = dump_yaml(graph_dict)

    # Write to files last to avoid partial updates
    location.write_text(content)
    graph_path.write_text(yaml)

    print(f"\n[success]Created node [b]{location}")
    print(
        f"\n[info]Once you've edited the node and are ready to run the graph, use [code]basis upload"
    )


_PY_FILE_TEMPLATE = """
from basis import *


@node
def {}(
    # Declare the node inputs and outputs here:
    # input_stream=InputStream,
    # output_table=OutputTable,
    # myparam=Parameter(type='text'),
):
    # use the inputs and outputs here:
    pass
"""

_SQL_FILE_TEMPLATE = """
create table {{ OutputTable("my_output_table") }}
select
    *
from {{ InputTable("other_node") }}
limit {{ Parameter("limit", "int", default=100) }}
"""
