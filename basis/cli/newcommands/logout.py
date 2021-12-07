import typer
from requests import HTTPError
from typer import Option

from basis.cli.newapp import app
from basis.cli.services import auth
from typer import secho
from typer import colors

from basis.cli.services.api import exit_on_http_error
from basis.cli.services.list import list_organizations, list_environments
from basis.cli.config import update_local_basis_config, get_basis_config_path
from click import Choice


@app.command()
def logout():
    """Log out of your Basis account"""
    auth.logout()
