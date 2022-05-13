from patterns.cli.config import (
    update_devkit_config,
    get_devkit_config_path,
)
from patterns.cli.services import login as login_service
from patterns.cli.services.accounts import me
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error


def login():
    """Log in to your Patterns account"""

    with abort_on_error("Login failed"):
        login_service.login()

    ids = IdLookup(ignore_local_cfg=True)
    with abort_on_error("Fetching account failed"):
        update_devkit_config(
            organization_id=ids.organization_id, environment_id=ids.environment_id
        )

    with abort_on_error("Fetching user profile failed"):
        profile = me()

    sprint(
        f"\n[success]Logged in to Patterns organization [b]{ids.organization_name}[/b] "
        f"as [b]{profile['username']}[/b] ([b]{profile['email']}[/b])"
    )
    sprint(
        f"\n[info]Your login information is stored at "
        f"{get_devkit_config_path().as_posix()}"
    )
    sprint(
        f"\n[info]If you want to create a new graph, run "
        f"[code]patterns create graph[/code] get started"
    )
