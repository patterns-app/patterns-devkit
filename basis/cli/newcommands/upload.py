import os
from pathlib import Path

import click
import typer
from typer import Option, Argument

from basis.cli.config import read_local_basis_config
from basis.cli.newapp import app
from basis.cli.services.api import abort_on_http_error
from basis.cli.services.deploy import deploy_graph_version
from basis.cli.services.output import print_success, print_info
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
        t = click.Path(exists=True, dir_okay=False)
        location = Path(typer.prompt(prompt, type=t)).absolute()
    else:
        location = cfg.default_graph

    with abort_on_http_error("Upload failed"):
        resp = upload_graph_version(location, organization or cfg.organization_name)

    print_success("\nUploaded new graph version.")

    if deploy:
        graph_version_id = resp["uid"]
        with abort_on_http_error("Deploy failed"):
            deploy_graph_version(graph_version_id, environment or cfg.environment_name)
        print_success(f"Graph deployed.")

    print_info(
        "\nVisit https://www.getbasis.com to view your graph"
    )  # TODO: use the actual UI endpoint
