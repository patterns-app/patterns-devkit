import io
import re
from pathlib import Path
from zipfile import ZipFile

import typer
from typer import Option, Argument

from patterns.cli.helpers import random_node_id
from patterns.cli.services.graph import resolve_graph_path
from patterns.cli.services.list import paginated_graph_components
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import abort, prompt_path, abort_on_error
from patterns.cli.services.output import sprint
from patterns.cli.services.pull import download_graph_zip
from patterns.configuration.edit import GraphConfigEditor
from patterns.configuration.edit import GraphDirectoryEditor

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
    with abort_on_error("Error creating graph"):
        path = resolve_graph_path(location, exists=False)
    name = name or location.stem
    GraphConfigEditor(path, read=False).set_name(name).write()

    sprint(f"\n[success]Created graph [b]{name}")
    sprint(
        f"\n[info]You can add nodes with [code]cd {location}[/code],"
        f" then [code]patterns create node[/code]"
    )


_graph_help = "The graph to add this node to"
_title_help = "The title of the node. The location will be used as a title by default"
_component_help = "The name of component to use to create this node"


@create.command()
def node(
    title: str = Option("", "--title", "-n", help=_name_help),
    component: str = Option("", "-c", "--component", help=_component_help),
    location: Path = Argument(None),
):
    """Add a new node to a graph

    patterns create node --name='My Node' mynode.py
    """
    if component and location:
        abort("Specify either a component or a node location, not both")

    if component:
        ids = IdLookup(find_nearest_graph=True)
        GraphConfigEditor(ids.graph_file_path).add_component_uses(
            component_key=component
        ).write()
        sprint(f"[success]Added component {component} to graph")
        return

    if not location:
        sprint("[info]Nodes can be python files like [code]ingest.py")
        sprint("[info]Nodes can be sql files like [code]aggregate.sql")
        sprint("[info]You also can add a subgraph like [code]processor/graph.yml")
        message = "Enter a name for the new node file"
        location = prompt_path(message, exists=False)

    if location.exists():
        abort(f"Cannot create node: {location} already exists")

    ids = IdLookup(node_file_path=location, find_nearest_graph=True)
    # Update the graph yaml
    node_file = "/".join(location.absolute().relative_to(ids.graph_directory).parts)
    node_title = title or (
        location.parent.name if location.name == "graph.yml" else location.stem
    )
    with abort_on_error("Adding node failed"):
        editor = GraphConfigEditor(ids.graph_file_path)
        editor.add_node(
            title=node_title, node_file=node_file, id=str(random_node_id()),
        )

    # Write to disk last to avoid partial updates
    if location.suffix == ".py":
        location.write_text(_PY_FILE_TEMPLATE)
    elif location.suffix == ".sql":
        location.write_text(_SQL_FILE_TEMPLATE)
    elif location.name == "graph.yml":
        location.parent.mkdir(exist_ok=True, parents=True)
        GraphConfigEditor(location, read=False).set_name(node_title).write()
    else:
        abort("Node file must be graph.yml or end in .py or .sql")
    editor.write()

    sprint(f"\n[success]Created node [b]{location}")
    sprint(
        f"\n[info]Once you've edited the node and are ready to run the graph, "
        f"use [code]patterns upload"
    )


_webhook_name_help = "The name of the webhook output stream"


@create.command()
def webhook(
    explicit_graph: Path = Option(None, "--graph", "-g", exists=True, help=_graph_help),
    name: str = Argument(..., help=_webhook_name_help),
):
    """Add a new webhook node to a graph"""
    ids = IdLookup(explicit_graph_path=explicit_graph)

    with abort_on_error("Adding webhook failed"):
        editor = GraphConfigEditor(ids.graph_file_path)
        editor.add_webhook(name, id=random_node_id())
        editor.write()

    sprint(f"\n[success]Created webhook [b]{name}")
    sprint(
        f"\n[info]Once you've deployed the graph, use "
        f"[code]patterns list webhooks[/code] to get the url of the webhook"
    )


_PY_FILE_TEMPLATE = """
# Documentation: https://docs.patterns.app/docs/node-development/python/

from patterns import (
    Parameter,
    State,
    Stream,
    Table,
)
"""

_SQL_FILE_TEMPLATE = """
-- Type '{{' to use Tables and Parameters
-- Documentation: https://docs.patterns.app/docs/node-development/sql/

select
"""
