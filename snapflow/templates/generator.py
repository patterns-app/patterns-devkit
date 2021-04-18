from pathlib import Path

from cookiecutter.main import cookiecutter

dir_path = Path(__file__).parent


def generate_template(template_name: str, **ctx):
    cookiecutter(
        str(dir_path / f"templates/{template_name}_template"),
        no_input=True,
        extra_context=ctx,
    )
