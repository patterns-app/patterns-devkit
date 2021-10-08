import os
import shutil
import re
from pathlib import Path

from cookiecutter.main import cookiecutter


dir_path = Path(__file__).parent
tmp_folder_name = "_tmp"


def generate_template(template_name: str, path: str, flatten: bool = False, **ctx):
    template_root = dir_path / f"templates/{template_name}"
    output_dir = Path(path).parent
    cookiecutter(
        str(template_root), no_input=True, extra_context=ctx, output_dir=output_dir
    )
    if flatten:
        flatten_files_remove_folder(path)


def flatten_files_remove_folder(path: str):
    for roots, dirs, files in os.walk(path):
        for f in files:
            if _should_ignore_file(f):
                continue
            os.rename(
                Path(path) / Path(f), Path(path) / ".." / Path(f).name,
            )
    shutil.rmtree(path)


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


def _should_ignore_file(path: str) -> bool:
    for ignored in [
        ".DS_Store",
        ".pytest_cache",
        ".egg-info",
        "__pycache__",
    ]:
        if ignored in path:
            return True
    return False
