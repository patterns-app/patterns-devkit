from basis.cli.config import (
    update_local_basis_config,
    get_basis_config_path,
)
from basis.cli.services import login as login_service
from basis.cli.services.accounts import me
from basis.cli.services.lookup import IdLookup
from basis.cli.services.output import sprint, abort_on_error


def login():
    """Log in to your Basis account"""

    with abort_on_error("Login failed"):
        login_service.login()

    ids = IdLookup(ignore_local_cfg=True)
    with abort_on_error("Fetching account failed"):
        update_local_basis_config(
            organization_id=ids.organization_id, environment_id=ids.environment_id
        )

    with abort_on_error("Fetching user profile failed"):
        profile = me()

    sprint(
        f"\n[success]Logged in to Basis organization [b]{ids.organization_name}[/b] "
        f"as [b]{profile['username']}[/b] ([b]{profile['email']}[/b])"
    )
    sprint(
        f"\n[info]Your login information is stored at "
        f"{get_basis_config_path().as_posix()}"
    )
    sprint(
        f"\n[info]If you want to create a new graph, run "
        f"[code]basis create graph[/code] get started"
    )
