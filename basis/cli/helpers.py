from __future__ import annotations

import os
import subprocess
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Generator


def _is_git_directory(path: Path) -> bool:
    return (path / ".git").is_dir()


def _all_files_not_gitignored(path: Path) -> Generator[Path]:
    files = subprocess.check_output(
        ["git", "-C", str(path), "ls-files", "-co", "--exclude-standard"]
    ).splitlines()
    for f in files:
        yield path / Path(f.decode())


def _all_files(path: Path) -> Generator[str]:
    for dirname, dirnames, files in os.walk(path):
        if "__pycache__" in dirnames:
            dirnames.remove("__pycache__")
        for f in files:
            p = Path(dirname) / f
            if p.suffix != ".pyc":
                yield p


def compress_directory(path: Path) -> BytesIO:
    io = BytesIO()
    zipf = zipfile.ZipFile(io, "w", zipfile.ZIP_DEFLATED)
    if _is_git_directory(path):
        # Respect .gitignore
        contents = _all_files_not_gitignored(path)
    else:
        contents = _all_files(path)
    for f in contents:
        zipf.write(f, f.relative_to(path))
    zipf.close()
    io.seek(0)
    io.name = "graph_manifest.zip"
    return io
