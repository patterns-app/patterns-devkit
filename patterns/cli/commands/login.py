from rich.progress import Progress, SpinnerColumn, TextColumn

from patterns.cli.config import (
    update_devkit_config,
    get_devkit_config_path,
)
from patterns.cli.services import login as login_service
from patterns.cli.services.accounts import me
from patterns.cli.services.api import reset_session_auth
from patterns.cli.services.lookup import IdLookup
from patterns.cli.services.output import sprint, abort_on_error, console


def login():
    """Log in to your Patterns account"""
    reset_session_auth()

    with abort_on_error("Login failed"):
        sprint("[info]Logging in to Patterns...")
        url, login_config = login_service.make_login_config()
        sprint(f"[info]Opening url:")
        sprint(url)

        progress = Progress(
            SpinnerColumn(),
            TextColumn("Waiting for authorization..."),
            console=console,
            transient=True,
        )
        with progress:
            progress.add_task("")
            login_service.login(url, login_config)

    ids = IdLookup(ignore_local_cfg=True)
    with abort_on_error("Saving authorization token failed"):
        update_devkit_config(organization_id=ids.organization_uid)

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
        f"\n[info]If you want to create a new app, run "
        f"[code]patterns create app[/code] get started"
    )
