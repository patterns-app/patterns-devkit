import typer
from click import Choice
from typer import Option

from basis.cli.config import (
    update_local_basis_config,
    get_basis_config_path,
    read_local_basis_config,
)
from basis.cli.newapp import app
from basis.cli.services import auth
from basis.cli.services.api import abort_on_http_error
from basis.cli.services.list import list_organizations, list_environments
from basis.cli.services.output import sprint, prompt_str

_email_help = "The email address of the account"
_password_help = "The password for the account"


@app.command()
def login(
    email: str = Option("", help=_email_help),
    password: str = Option("", help=_password_help),
):
    """Log in to your Basis account"""
    if not email:
        email = prompt_str("Email", default=read_local_basis_config().email)

    if not password:
        password = prompt_str("Password", password=True)

    with abort_on_http_error("Login failed"):
        auth.login(email, password)

    with abort_on_http_error("Fetching organizations failed"):
        organizations = list_organizations()

    if len(organizations) == 1:
        org_name = organizations[0]["name"]
    else:
        org_name = prompt_str(
            "Select an organization", choices=[org["name"] for org in organizations],
        )

    with abort_on_http_error("Fetching environments failed"):
        environments = list_environments()

    if len(environments) == 1:
        env_name = environments[0]["name"]
    elif environments:
        env_name = typer.prompt(
            "Select an organization", type=Choice([env["name"] for env in environments])
        )
    else:
        env_name = None

    update_local_basis_config(organization_name=org_name, environment_name=env_name)
    sprint(
        f"\n[success]Logged in to Basis organization [b]{org_name}[/b] as [b]{email}"
    )
    sprint(f"\n[info]Your login information is stored at {get_basis_config_path()}")
    sprint(
        f"\n[info]If you want to create a new graph, run [code]basis create graph[/code] get started"
    )
