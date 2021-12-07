import typer
from typer import secho
import typing


def abort(message: str) -> typing.NoReturn:
    """Print an error message and raise an Exit exception"""
    print_err(message)
    raise typer.Exit(1)


def print_err(message: str):
    """Print a message colored with the error style"""
    secho(message, fg="red")


def print_success(message: str):
    """Print a message colored with the success style"""
    secho(message, fg="green")


def print_info(message: str):
    """Print a message colored with the info style"""
    secho(message, fg="cyan")
