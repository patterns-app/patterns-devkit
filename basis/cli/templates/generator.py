import os
import shutil
import re
from pathlib import Path

from cookiecutter.main import cookiecutter


dir_path = Path(__file__).parent
tmp_folder_name = "_tmp"


def generate_template(template_name: str, flatten: bool = False, **ctx):
    template_root = dir_path / f"templates/{template_name}_template"
    cookiecutter(
        str(template_root), no_input=True, extra_context=ctx,
    )
    if flatten:
        flatten_files_remove_folder(template_root)


def flatten_files_remove_folder(pth: Path):
    tmp_dir = Path(os.path.curdir) / tmp_folder_name
    for roots, dirs, files in os.walk(tmp_dir):
        for f in files:
            os.rename(
                Path(f), Path(f) / "..",
            )
    shutil.rmtree(tmp_dir)


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
