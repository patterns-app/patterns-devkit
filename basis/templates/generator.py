import re
from pathlib import Path
from typing import Pattern

from cookiecutter.main import cookiecutter

dir_path = Path(__file__).parent


def generate_template(template_name: str, **ctx):
    cookiecutter(
        str(dir_path / f"templates/{template_name}_template"),
        no_input=True,
        extra_context=ctx,
    )


def insert_into_file(pth: str, insert: str, after: str):
    with open(pth, "r") as f:
        s = f.read()
        matches = list(re.finditer(after, s))
        if not matches:
            s = s + "\n" + insert + "\n"
        else:
            last_match = matches[-1]
            s = s[0 : last_match.end()] + f"\n{insert}\n" + s[(last_match.end() + 1) :]
    with open(pth, "w") as f:
        f.write(s)
