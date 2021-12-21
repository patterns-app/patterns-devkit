import contextlib
import typing
from pathlib import Path

import rich.prompt
import typer
from requests import HTTPError
from rich.console import Console
from rich.theme import Theme

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


def prompt_str(
    message: str,
    default: str = None,
    password: bool = False,
    choices: typing.List[str] = None,
) -> str:
    return rich.prompt.Prompt.ask(
        message, default=default, password=password, choices=choices
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
    try:
        yield
    except HTTPError as e:
        try:
            details = e.response.json()["detail"]
        except Exception:
            details = e.response.text
        abort(f"{message}{prefix}{details}{suffix}")
    except (typer.Exit, typer.Abort) as e:
        raise e
    except Exception as e:
        abort(f"{message}{prefix}{e}{suffix}")
