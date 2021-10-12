import os
import subprocess
from pathlib import Path
from typing import List, Union
import zipfile
from io import BytesIO

PathLike = Union[Path, str]


def is_git_directory(path: PathLike) -> bool:
    return os.path.exists(Path(path) / ".git")


def list_all_files_not_gitignored(path: PathLike) -> List[str]:
    os.chdir(path)
    cmd = "( git status --short| grep '^?' | cut -d\  -f2- && git ls-files )"
    files = subprocess.check_output(cmd, shell=True).splitlines()
    return [b.decode() for b in files]


def list_all_files(path: PathLike) -> List[str]:
    (_, _, filenames) = next(os.walk(path))
    return filenames


def compress_directory(path: PathLike) -> BytesIO:
    io = BytesIO()
    zipf = zipfile.ZipFile(io, "w", zipfile.ZIP_DEFLATED)
    if is_git_directory(path):
        # Respect .gitignore
        all_files = list_all_files_not_gitignored(path)
    else:
        all_files = list_all_files(path)
    for f in all_files:
        # TODO: paths...
        zipf.write(f, os.path.relpath(os.path.join(path, f), os.path.join(path, "..")))
    return io
