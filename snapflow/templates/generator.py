from pathlib import Path
from cookiecutter.main import cookiecutter

dir_path = Path(__file__).parent


def generate_module(**ctx):
    cookiecutter(
        str(dir_path / "templates/module_template"), no_input=True, extra_context=ctx
    )


def generate_snap(**ctx):
    cookiecutter(
        str(dir_path / "templates/snap_template"), no_input=True, extra_context=ctx
    )


def generate_schema(**ctx):
    cookiecutter(
        str(dir_path / "templates/schema_template"), no_input=True, extra_context=ctx
    )
