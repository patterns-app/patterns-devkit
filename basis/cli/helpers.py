import os
import subprocess
from pathlib import Path
from typing import List, Union
import zipfile
from io import BytesIO


def list_all_files_not_gitignored(path: Union[Path, str]) -> List[str]:
    os.chdir(path)
    cmd = "( git status --short| grep '^?' | cut -d\  -f2- && git ls-files )"
    files = subprocess.check_output(cmd, shell=True).splitlines()
    return [b.decode() for b in files]


def compress_directory(path: Union[Path, str]) -> BytesIO:
    io = BytesIO()
    zipf = zipfile.ZipFile(io, "w", zipfile.ZIP_DEFLATED)
    for f in list_all_files_not_gitignored(path):
        # TODO: paths...
        zipf.write(f, os.path.relpath(os.path.join(path, f), os.path.join(path, "..")))
    return io

