from __future__ import annotations

import os
from contextlib import contextmanager
from io import BufferedIOBase, IOBase, TextIOBase
from typing import ContextManager, Generator, Iterable, Iterator, TextIO, Type

from snapflow.core.data_block import DataBlockMetadata, StoredDataBlockMetadata
from snapflow.core.data_formats import RecordsList
from snapflow.core.environment import Environment
from snapflow.core.storage.storage import (
    FileSystemStorageManager,
    Storage,
    StorageEngine,
)
from snapflow.utils.common import to_json
from snapflow.utils.data import write_csv


class FileSystemAPI:
    def __init__(self, env: Environment, storage: Storage):
        self.env = env
        self.storage = storage

    @contextmanager
    def open(
        self, stored_data_block: StoredDataBlockMetadata, *args, **kwargs
    ) -> Iterator[TextIO]:
        with open(self.get_path(stored_data_block), *args, **kwargs) as f:
            yield f

    def get_path(self, stored_data_block: StoredDataBlockMetadata) -> str:
        fname = stored_data_block.get_name(self.env)
        dir = self.storage.url[7:]
        return os.path.join(dir, fname)

    def exists(self, stored_data_block: StoredDataBlockMetadata) -> bool:
        return os.path.exists(self.get_path(stored_data_block))

    def write_records_to_csv(
        self,
        stored_data_block: StoredDataBlockMetadata,
        records_iterable: Iterable[RecordsList],
    ):
        output_schema = stored_data_block.realized_schema(self.env)
        columns = [f.name for f in output_schema.fields]
        with self.open(stored_data_block, "w") as f:
            append = False
            for records in records_iterable:
                write_csv(records, f, columns=columns, append=append)
                append = True

    def write_records_as_json(
        self,
        stored_data_block: StoredDataBlockMetadata,
        records_iterable: Iterable[RecordsList],
    ):
        with self.open(stored_data_block, "w") as f:
            for records in records_iterable:
                lines = []
                for record in records:
                    j = to_json(record)
                    lines.append(j + "\n")
                f.writelines(lines)

    def create_alias(self, from_name: str, to_name: str):
        try:
            os.remove(to_name)
        except FileNotFoundError:
            pass
        os.symlink(from_name, to_name)


# TODO: better way to register these types of managers / apis (so someone can extend without editing
def get_file_system_api_class(engine: StorageEngine) -> Type[FileSystemAPI]:
    return {StorageEngine.LOCAL: FileSystemAPI}.get(engine, FileSystemAPI)
