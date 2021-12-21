from typer import Option

from basis.cli.config import (
    update_local_basis_config,
    get_basis_config_path,
    read_local_basis_config,
)
from basis.cli.services import auth
from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import sprint, prompt_str, abort_on_error

_email_help = "The email address of the account"
_password_help = "The password for the account"


def login(
    email: str = Option("", help=_email_help),
    password: str = Option("", help=_password_help),
):
    """Log in to your Basis account"""
    if not email:
        email = prompt_str("Email", default=read_local_basis_config().email)

    if not password:
        password = prompt_str("Password", password=True)

    with abort_on_error("Login failed"):
        auth.login(email, password)

    ids = IdLookup(ignore_local_cfg=True)
    with abort_on_error("Fetching account failed"):
        update_local_basis_config(
            organization_id=ids.organization_id, environment_id=ids.environment_id
        )
    sprint(
        f"\n[success]Logged in to Basis organization [b]{ids.organization_name}[/b] "
        f"as [b]{email}"
    )
    sprint(f"\n[info]Your login information is stored at {get_basis_config_path()}")
    sprint(
        f"\n[info]If you want to create a new graph, run "
        f"[code]basis create graph[/code] get started"
    )
