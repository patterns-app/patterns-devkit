from pathlib import Path

import typer
from typer import Option, Argument

from basis.cli.config import (
    read_local_basis_config,
    write_local_basis_config,
)
from basis.cli.services.graph import find_graph_file, resolve_graph_path
from basis.cli.services.output import abort, prompt_path, abort_on_error
from basis.cli.services.output import sprint
from basis.cli.services.paths import is_relative_to
from basis.configuration.base import dump_yaml
from basis.configuration.edit import GraphConfigEditor
from basis.configuration.path import NodeId

create = typer.Typer(name="create", help="Create a graph new or node")

_name_help = "The name of the graph. The location will be used as a name by default"


@create.command()
def graph(
    name: str = Option("", "--name", "-n", help=_name_help),
    location: Path = Argument(None, metavar="GRAPH"),
):
    """Add a new node to a graph"""
    if not location:
        prompt = (
            "Enter a name for the new graph directory [prompt.default](e.g. my_graph)"
        )
        location = prompt_path(prompt, exists=False)

    cfg = read_local_basis_config()
    path = resolve_graph_path(location, exists=False)
    name = name or location.stem
    path.write_text(dump_yaml({"name": name}))
    write_local_basis_config(cfg)

    sprint(f"\n[success]Created graph [b]{name}")
    sprint(
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
        message = "Enter a name for the new node file [prompt.default](e.g. mynode.sql)"
        location = prompt_path(message, exists=False)

    if location.exists():
        abort(f"{location} already exists")

    if location.suffix == ".py":
        content = _PY_FILE_TEMPLATE.format(location.stem)
    elif location.suffix == ".sql":
        content = _SQL_FILE_TEMPLATE
    else:
        abort("Node file location must end in .py or .sql")

    graph_path = find_graph_file(explicit_graph or location.parent)
    graph_dir = graph_path.parent
    if not location.is_absolute() and not is_relative_to(
        location.absolute(), graph_dir
    ):
        sprint(f"\n[error]Node location is not in the graph directory.")
        sprint(
            f"\n[info]Try changing your directory to the graph directory "
            f"[code]({graph_dir})"
        )
        sprint(
            f"[info]You can change the graph directory for this command with the "
            f"[code]--graph[/code] option"
        )
        raise typer.Exit(1)

    # Update the graph yaml
    node_file = "/".join(location.absolute().relative_to(graph_dir).parts)
    with abort_on_error("Adding node failed"):
        editor = GraphConfigEditor(graph_path)
        editor.add_node(
            name=name or location.stem, node_file=node_file, id=str(NodeId.random()),
        )

    # Write to files last to avoid partial updates
    location.write_text(content)
    editor.write()

    sprint(f"\n[success]Created node [b]{location}")
    sprint(
        f"\n[info]Once you've edited the node and are ready to run the graph, "
        f"use [code]basis upload"
    )


_webhook_name_help = "The name of the webhook output stream"


@create.command()
def webhook(
    explicit_graph: Path = Option(None, "--graph", "-g", exists=True, help=_graph_help),
    name: str = Argument(..., help=_webhook_name_help),
):
    """Add a new webhook node to a graph"""
    graph_path = find_graph_file(explicit_graph)

    with abort_on_error("Adding webhook failed"):
        editor = GraphConfigEditor(graph_path)
        editor.add_webhook(name, id=str(NodeId.random()))
        editor.write()

    sprint(f"\n[success]Created webhook [b]{name}")
    sprint(
        f"\n[info]Once you've deployed the graph, use "
        f"[code]basis list webhooks[/code] to get the url of the webhook"
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
