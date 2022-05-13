import contextlib
import typing
from pathlib import Path

import rich.prompt
import typer
from requests import HTTPError
from rich.console import Console
from rich.theme import Theme

"""Set to True to raise exceptions from cli commands rather than printing the message"""
DEBUG = False

console = Console(
    theme=(
        Theme(
            {
                "info": "italic cyan",
                "warning": "magenta",
                "success": "green",
                "error": "red",
                "code": "bold cyan",
            }
        )
    )
)


class _PathPrompt(rich.prompt.Prompt):
    response_type = Path
    validate_error_message = "[prompt.invalid]Please enter a valid file path"


def prompt_path(
    message: str, default: typing.Union[Path, str] = None, exists: bool = None
) -> Path:
    while True:
        p = _PathPrompt.ask(message, default=default)
        if not p:
            continue
        if exists is True and not p.exists():
            sprint("[prompt.invalid]Path already exists")
        elif exists is False and p.exists():
            sprint("[prompt.invalid]Path does not exist")
        else:
            break
    return p


def prompt_str(message: str, default: str = None, password: bool = False) -> str:
    return rich.prompt.Prompt.ask(message, default=default, password=password)


def prompt_choices(
    choice_message: str,
    prompt_message: str,
    choices: typing.Iterable[str],
) -> str:
    sprint(f"[info]{choice_message}:")
    for c in choices:
        sprint(f"    [info]{c}")
    return rich.prompt.Prompt.ask(
        prompt_message,
        choices=list(choices),
        show_choices=False,
    )


def sprint(message):
    """Print styled content"""
    console.print(message)


def abort(message: str) -> typing.NoReturn:
    """Print an error message and raise an Exit exception"""
    sprint(f"[error]{message}")
    raise typer.Exit(1)


@contextlib.contextmanager
def abort_on_error(message: str, prefix=": ", suffix=""):
    """Catch any exceptions that occur and call `abort` with their message"""
    if DEBUG:
        yield
        return
    try:
        yield
    except HTTPError as e:
        try:
            details = e.response.json()["detail"]
        except Exception:
            details = e.response.text
        if not details:
            details = f"HTTP {e.response.status_code}"

        # check 403 error message for unverified email / unsetup account and display message
        # we give them the home page, since the webapp will redirect them to the proper setup page automatically
        if e.response.status_code == 403:
            if details == "unverified email":
                abort(
                    f"Please verify your email address before using Patterns - https://studio.patterns.app"
                )
            elif details == "incomplete setup":
                abort(
                    f"Please finish account setup before using Patterns - https://studio.patterns.app"
                )
        elif e.response.status_code == 401:
            abort(
                "You are not logged in to the devkit.\n"
                "[info]You can log in with [code]patterns login"
            )

        abort(f"{message}{prefix}{details}{suffix}")
    except (typer.Exit, typer.Abort) as e:
        raise e
    except KeyError as e:
        abort(f"{message}{prefix}KeyError: {e}{suffix}")
    except Exception as e:
        abort(f"{message}{prefix}{e}{suffix}")
