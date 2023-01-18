import re
from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from typer import Option, Argument

from patterns.cli.configuration.edit import GraphConfigEditor
from patterns.cli.helpers import random_node_id
from patterns.cli.services.graph_path import resolve_graph_path
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import abort, prompt_path, abort_on_error, prompt_str
from patterns.cli.services.output import sprint
from patterns.cli.services.secrets import create_secret

create = typer.Typer(name="create", help="Create a new app or node")

_name_help = "The name of the app. The location will be used as a name by default"


@create.command()
def app(
    name: str = Option("", "--name", "-n", help=_name_help),
    location: Path = Argument(None, metavar="APP"),
):
    """Create a new empty app"""
    if not location:
        prompt = "Enter a name for the new app directory [prompt.default](e.g. my-app)"
        location = prompt_path(prompt, exists=False)
    with abort_on_error("Error creating app"):
        path = resolve_graph_path(location, exists=False)
    name = name or location.stem
    slug = re.sub("[_ ]+", "-", name)
    slug = re.sub("[^a-zA-Z0-9-]+", "", slug).lower()
    GraphConfigEditor(path, read=False).set_name(name).set_slug(slug).write()

    sprint(f"\n[success]Created app [b]{name}")
    sprint(
        f"\n[info]You can add nodes with [code]cd {location}[/code],"
        f" then [code]patterns create node[/code]"
    )


_app_help = "The app to add this node to (defaults to the app in the current directory)"
_title_help = "The title of the node. The location will be used as a title by default"
_component_help = "The name of component to use to create this node"
_type_help = "The type of node to create"
_location_help = "The file to create for the node (for function nodes), or the name of the component or webhook"


class _NodeType(str, Enum):
    function = "function"
    component = "component"
    webhook = "webhook"


@create.command()
def node(
    explicit_app: Path = Option(None, "--app", "-a", exists=True, help=_app_help),
    title: str = Option("", "--title", "-n", help=_name_help),
    component: str = Option("", "-c", "--component", help=_component_help, hidden=True),
    type: _NodeType = Option(_NodeType.function, help=_type_help),
    location: str = Argument("", help=_location_help),
):
    """Add a new node to an app

    patterns create node --name='My Node' mynode.py
    """
    # --component option is deprecated
    if component and location:
        abort("Specify either a component or a node location, not both")
    if component:
        _add_component_node(explicit_app, component, title)
        return

    if type == _NodeType.function:
        location = Path(location) if location else location
        _add_function_node(explicit_app, location, title)
    elif type == _NodeType.component:
        _add_component_node(explicit_app, location, title)
    elif type == _NodeType.webhook:
        _add_webhook_node(explicit_app, location, title)
    else:
        raise NotImplementedError(f"Unexpected node type {type}")


def _add_component_node(explicit_app: Optional[Path], component: str, title: str):
    if not component:
        sprint("[info]Components names look like [code]patterns/component@v1")
        component = prompt_str("Enter the name of the component to add")
    ids = IdLookup(find_nearest_graph=True, graph_path=explicit_app)
    GraphConfigEditor(ids.graph_file_path).add_component_uses(
        component_key=component, title=title
    ).write()
    sprint(f"[success]Added component {component} to app")


def _add_function_node(
    explicit_app: Optional[Path], location: Optional[Path], title: str
):
    if not location:
        sprint("[info]Nodes can be python files like [code]ingest.py")
        sprint("[info]Nodes can be sql files like [code]aggregate.sql")
        sprint("[info]You also can add a subgraph like [code]processor/graph.yml")
        location = prompt_path("Enter a name for the new node file", exists=False)

    if location.exists():
        abort(f"Cannot create node: {location} already exists")

    ids = IdLookup(
        node_file_path=location, find_nearest_graph=True, graph_path=explicit_app
    )
    # Update the graph yaml
    node_file = "/".join(location.absolute().relative_to(ids.graph_directory).parts)
    node_title = title or (
        location.parent.name if location.name == "graph.yml" else location.stem
    )
    with abort_on_error("Adding node failed"):
        editor = GraphConfigEditor(ids.graph_file_path)
        editor.add_node(
            title=node_title,
            node_file=node_file,
            id=str(random_node_id()),
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
        f"\n[info]Once you've edited the node and are ready to run the app, "
        f"use [code]patterns upload"
    )


# deprecated
@create.command(hidden=True)
def webhook(
    explicit_app: Path = Option(None, "--app", "-a", exists=True, help=_app_help),
    name: str = Argument(..., help="The name of the webhook output stream"),
):
    """Add a new webhook node to an app"""
    _add_webhook_node(explicit_app, name, None)


def _add_webhook_node(explicit_app: Optional[Path], name: str, title: Optional[str]):
    ids = IdLookup(graph_path=explicit_app)

    if not name:
        name = prompt_str("Enter the table that the webhook will write to")

    with abort_on_error("Adding webhook failed"):
        editor = GraphConfigEditor(ids.graph_file_path)
        editor.add_webhook(name, id=random_node_id(), title=title)

        # Add the output table if it doesn't exist already
        if not any(n.get("table") == name for n in editor.store_nodes()):
            editor.add_store(name, id=random_node_id())

        editor.write()

    sprint(f"\n[success]Created webhook [b]{name}")
    sprint(
        f"\n[info]Once you've uploaded the app, use "
        f"[code]patterns list webhooks[/code] to get the url of the webhook"
    )


_organization_help = "The Patterns organization to add a secret to"
_secret_name_help = (
    "The name of the secret. Can only contain letters, numbers, and underscores."
)
_secret_value_help = "The value of the secret"
_secret_desc_help = "A description for the secret"
_sensitive_help = "Mark the secret value as sensitive. This value won't be visible to the UI or devkit."


@create.command()
def secret(
    organization: str = Option(
        "", "-o", "--organization", metavar="SLUG", help=_organization_help
    ),
    sensitive: bool = Option(False, "--sensitive", "-s", help=_sensitive_help),
    description: str = Option(None, "-d", "--description", help=_secret_desc_help),
    name: str = Argument(..., help=_secret_name_help),
    value: str = Argument(..., help=_secret_value_help),
):
    """Create a new secret value in your organization"""
    ids = IdLookup(organization_slug=organization)

    with abort_on_error("Creating secret failed"):
        create_secret(ids.organization_uid, name, value, description, sensitive)
    sprint(f"\n[success]Created secret [b]{name}")


_PY_FILE_TEMPLATE = """
# Documentation: https://docs.patterns.app/docs/node-development/python/

from patterns import (
    Parameter,
    State,
    Table,
)
"""

_SQL_FILE_TEMPLATE = """
-- Type '{{' to use Tables and Parameters
-- Documentation: https://docs.patterns.app/docs/node-development/sql/

select
"""
