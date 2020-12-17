from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from itertools import repeat, takewhile
from typing import ContextManager, Generator, Iterable, Iterator, Optional, TextIO, Type

from snapflow.storage.storage import Storage, StorageApi, StorageEngine


def raw_line_count(filename: str) -> int:
    # Fast file cnt in python
    # From: https://stackoverflow.com/questions/845058/how-to-get-line-count-of-a-large-file-cheaply-in-python
    # TODO: won't count EOF if no new line
    f = open(filename, "rb")
    bufgen = takewhile(lambda x: x, (f.raw.read(1024 * 1024) for _ in repeat(None)))
    return sum(buf.count(b"\n") for buf in bufgen)


class FileSystemStorageApi(StorageApi):
    def __init__(self, storage: Storage):
        self.storage = storage

    @contextmanager
    def open(self, name: str, *args, **kwargs) -> Iterator[TextIO]:
        with open(self.get_path(name), *args, **kwargs) as f:
            yield f

    def get_path(self, name: str) -> str:
        dir = self.storage.url[7:]
        return os.path.join(dir, name)

    ### StorageApi implementations ###
    def exists(self, name: str) -> bool:
        return os.path.exists(self.get_path(name))

    def create_alias(self, name: str, alias: str):
        pth = self.get_path(name)
        alias_pth = self.get_path(alias)
        try:
            os.remove(alias_pth)
        except FileNotFoundError:
            pass
        os.symlink(pth, alias_pth)

    def record_count(self, name: str) -> Optional[int]:
        # TODO: this depends on format... hmmm, i guess let upstream handle for now
        pth = self.get_path(name)
        return raw_line_count(pth)

    def copy(self, name: str, to_name: str):
        pth = self.get_path(name)
        to_pth = self.get_path(to_name)
        shutil.copy(pth, to_pth)

    def write_lines_to_file(
        self,
        name: str,
        lines: Iterable[str],  # TODO: support bytes?
    ):
        with self.open(name, "w") as f:
            f.writelines(ln + "\n" for ln in lines)
