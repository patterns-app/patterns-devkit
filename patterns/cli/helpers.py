from __future__ import annotations

import os
import re
import secrets
import string
import subprocess
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Generator

# standard gitignore entries from https://github.com/github/gitignore
_IGNORE_DIRS = {
    ".com.apple.timemachine.donotpresent",
    ".DocumentRevisions-V100",
    ".DS_Store",
    ".eggs",
    ".fseventsd",
    ".hypothesis",
    ".idea",
    ".mypy_cache",
    ".nox",
    ".pybuilder",
    ".pyre",
    ".pytest_cache",
    ".pytype",
    ".Spotlight-V100",
    ".TemporaryItems",
    ".tox",
    ".Trashes",
    ".VolumeIcon.icns",
    ".vscode",
    "__pycache__",
    "__pypackages__",
    "cython_debug",
    "develop-eggs",
    "docs_build",
    "ENV",
    "htmlcov",
    "instance",
    "profile_default",
    "sharepython-wheels",
}
_IGNORE_FILES = [
    r".*\$py\.class",
    r".*\.cover",
    r".*\.egg",
    r".*\.log",
    r".*\.manifest",
    r".*\.mo",
    r".*\.pot",
    r".*\.py,cover",
    r".*\.py[cod]",
    r".*\.sage\.py",
    r".*\.so",
    r".*\.spec",
    r"\.cache",
    r"\.coverage",
    r"\.coverage\..*",
    r"\.dmypy\.json",
    r"\.env",
    r"\.installed\.cfg",
    r"\.ipynb_checkpoints",
    r"\.pdm\.toml",
    r"\.Python",
    r"\.ropeproject",
    r"\.scrapy",
    r"\.spyderproject",
    r"\.spyproject",
    r"\.venv",
    r"\.webassets-cache",
    r"celerybeat-schedule",
    r"celerybeat\.pid",
    r"coverage\.xml",
    r"db\.sqlite3",
    r"db\.sqlite3-journal",
    r"dmypy\.json",
    r"ipython_config\.py",
    r"MANIFEST",
    r"nosetests\.xml",
    r"pip-delete-this-directory\.txt",
    r"pip-log\.txt",
]

_IGNORE_RE = re.compile(f"(?:{'|'.join(_IGNORE_FILES)})$")


def _is_git_directory(path: Path) -> bool:
    return (path / ".git").is_dir()


def _all_files_not_gitignored(path: Path) -> Generator[Path]:
    files = subprocess.check_output(
        ["git", "-C", str(path), "ls-files", "-co", "--exclude-standard"]
    ).splitlines()
    for f in files:
        yield path / Path(f.decode())


def _all_files_not_ignored(path: Path) -> Generator[str]:
    for dirname, dirnames, files in os.walk(path, followlinks=True):
        dirnames[:] = [d for d in dirnames if d not in _IGNORE_DIRS]
        for f in files:
            if _IGNORE_RE.fullmatch(f):
                continue
            yield Path(dirname) / f


def compress_directory(path: Path) -> BytesIO:
    io = BytesIO()
    zipf = zipfile.ZipFile(io, "w", zipfile.ZIP_DEFLATED)
    if _is_git_directory(path):
        # Respect .gitignore
        contents = _all_files_not_gitignored(path)
    else:
        contents = _all_files_not_ignored(path)
    for f in contents:
        zipf.write(f, f.relative_to(path))
    zipf.close()
    io.seek(0)
    io.name = "graph_manifest.zip"
    return io


_alphabet = string.digits + string.ascii_lowercase


def random_node_id() -> str:
    return "".join(secrets.choice(_alphabet) for _ in range(8))
