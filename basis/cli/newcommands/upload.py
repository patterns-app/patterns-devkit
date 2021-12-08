import os
from pathlib import Path

import click
import typer
from typer import Option, Argument

from basis.cli.config import read_local_basis_config, resolve_graph_path
from basis.cli.newapp import app
from basis.cli.services.api import abort_on_http_error
from basis.cli.services.deploy import deploy_graph_version
from basis.cli.services.output import print, prompt_path
from basis.cli.services.upload import upload_graph_version

_graph_help = "The location of the graph.yml file for the graph to upload"
_deploy_help = "Whether or not to automatically deploy the graph after upload"
_organization_help = "The name of the Basis organization to upload to"
_environment_help = "The name of the Basis environment to use if deploying the graph"


@app.command()
def upload(
    deploy: bool = Option(True, "--deploy/--no-deploy", help=_deploy_help),
    organization: str = Option("", help=_organization_help),
    environment: str = Option("", help=_environment_help),
    graph: Path = Argument(None, exists=True, help=_graph_help),
):
    """Upload a new version of a graph to Basis"""
    cfg = read_local_basis_config()
    cwd = Path(os.getcwd()).absolute()
    if not graph and not cwd.is_relative_to(cfg.default_graph.parent):
        prompt = "Enter the location of the graph.yml file"
        location = prompt_path(prompt, exists=True).absolute()
        location = resolve_graph_path(location, exists=True)
    else:
        location = cfg.default_graph

    with abort_on_http_error("Upload failed"):
        resp = upload_graph_version(location, organization or cfg.organization_name)

    graph_version_id = resp["uid"]
    print(f"\n[success]Uploaded new graph version with id [b]{graph_version_id}.")

    if deploy:
        with abort_on_http_error("Deploy failed"):
            deploy_graph_version(graph_version_id, environment or cfg.environment_name)
        print(f"[success]Graph deployed.")

    print(
        "\n[info]Visit [code]https://www.getbasis.com[/code] to view your graph"
    )  # TODO: use the actual UI endpoint
