import io
import re
from pathlib import Path
from zipfile import ZipFile

import typer
from typer import Option, Argument

from basis.cli.services.graph import resolve_graph_path
from basis.cli.services.list import graph_components_all
from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import abort, prompt_path, abort_on_error
from basis.cli.services.output import sprint
from basis.cli.services.paths import is_relative_to
from basis.cli.services.pull import download_graph_zip
from basis.configuration.edit import GraphConfigEditor
from basis.configuration.edit import GraphDirectoryEditor
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
    with abort_on_error("Error creating graph"):
        path = resolve_graph_path(location, exists=False)
    name = name or location.stem
    GraphConfigEditor(path, read=False).set_name(name).write()

    sprint(f"\n[success]Created graph [b]{name}")
    sprint(
        f"\n[info]You can add nodes with [code]cd {location}[/code],"
        f" then [code]basis create node[/code]"
    )


_graph_help = "The graph to add this node to"
_name_help = "The name of the node. The location will be used as a name by default"
_component_help = "The name of component to use to create this node"


@create.command()
def node(
    name: str = Option("", "--name", "-n", help=_name_help),
    component: str = Option("", "-c", "--component", help=_component_help),
    location: Path = Argument(None),
):
    """Add a new node to a graph

    basis create node --name='My Node' mynode.py
    """
    if not location:
        sprint("[info]Nodes can be python files like [code]ingest.py")
        sprint("[info]Nodes can be sql files like [code]aggregate.sql")
        sprint("[info]You also can add a subgraph like [code]processor/graph.yml")
        message = "Enter a name for the new node file"
        location = prompt_path(message, exists=False)

    if location.exists():
        abort(f"Cannot create node: {location} already exists")

    ids = IdLookup(node_file_path=location)
    graph_dir = ids.graph_file_path.parent

    # Update the graph yaml
    node_file = "/".join(location.absolute().relative_to(graph_dir).parts)
    node_name = name or (
        location.parent.name if location.name == "graph.yml" else location.stem
    )
    with abort_on_error("Adding node failed"):
        editor = GraphConfigEditor(ids.graph_file_path)
        editor.add_node(
            name=node_name, node_file=node_file, id=str(NodeId.random()),
        )

    # Write to disk last to avoid partial updates
    if component:
        with abort_on_error("Adding component failed"):
            search = (r for r in graph_components_all() if r["name"] == component)
            component_resp = next(search, None)
            if not component_resp:
                sprint(f"[error]No component named {component}")
                sprint(
                    f"[info]Run [b]basis list components[/b] "
                    f"for a list of available components"
                )
                raise typer.Exit(1)

            zip_src_path = component_resp["file_path"]
            if zip_src_path.endswith("graph.yml") and location.name != "graph.yml":
                sprint(f"[error]Component {component} is a subgraph")
                norm = re.sub(r"\W", "_", component)
                suggestion = f"{norm}/graph.yml"
                sprint(
                    f"\n[info]For subgraph components, you need to pass the "
                    f"location of the graph.yml file to create"
                )
                sprint(
                    f"[info]Try [code]basis create node "
                    f"--component='{component}' {suggestion}"
                )
                raise typer.Exit(1)

            b = io.BytesIO(download_graph_zip(component_resp["graph_version_uid"]))
            dir_editor = GraphDirectoryEditor(ids.graph_file_path, overwrite=False)
            with ZipFile(b, "r") as zf:
                dir_editor.add_node_from_zip(zip_src_path, location, zf)
    else:
        if location.suffix == ".py":
            fun_name = re.sub(r"[^a-zA-Z0-9_]", "_", location.stem)
            if re.match(r"\d", fun_name):
                fun_name = f"node_{fun_name}"
            location.write_text(_PY_FILE_TEMPLATE.format(fun_name))
        elif location.suffix == ".sql":
            location.write_text(_SQL_FILE_TEMPLATE)
        elif location.name == "graph.yml":
            location.parent.mkdir(exist_ok=True, parents=True)
            GraphConfigEditor(location, read=False).set_name(node_name).write()
        else:
            abort("Node file must be graph.yml or end in .py or .sql")
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
    ids = IdLookup(explicit_graph_path=explicit_graph)

    with abort_on_error("Adding webhook failed"):
        editor = GraphConfigEditor(ids.graph_file_path)
        editor.add_webhook(name, id=NodeId.random())
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
