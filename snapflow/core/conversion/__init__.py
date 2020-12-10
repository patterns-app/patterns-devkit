from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Type

from loguru import logger
from snapflow.core.conversion.converter import (
    ConversionPath,
    Converter,
    ConverterLookup,
    StorageFormat,
)
from snapflow.core.data_block import StoredDataBlockMetadata
from snapflow.core.data_formats import DataFormat
from snapflow.core.storage.storage import Storage, StorageType
from snapflow.utils.common import printd

if TYPE_CHECKING:
    from snapflow.core.runnable import ExecutionContext


third_party_converters: List[Type[Converter]] = []


def register_converter(converter: Type[Converter]):
    third_party_converters.append(converter)


def get_converter_lookup() -> ConverterLookup:
    from snapflow.core.conversion.database_to_database import (
        DatabaseToDatabaseConverter,
    )
    from snapflow.core.conversion.database_to_memory import DatabaseToMemoryConverter
    from snapflow.core.conversion.memory_to_database import MemoryToDatabaseConverter
    from snapflow.core.conversion.memory_to_memory import MemoryToMemoryConverter
    from snapflow.core.conversion.memory_to_file import MemoryToFileConverter
    from snapflow.core.conversion.file_to_memory import FileToMemoryConverter

    lookup = ConverterLookup()
    lookup.add(MemoryToDatabaseConverter)
    lookup.add(MemoryToMemoryConverter)
    lookup.add(DatabaseToMemoryConverter)
    lookup.add(DatabaseToDatabaseConverter)
    lookup.add(MemoryToFileConverter)
    lookup.add(FileToMemoryConverter)
    for converter in third_party_converters:
        lookup.add(converter)
    return lookup


class ConversionPathDoesNotExist(Exception):
    pass


def convert_lowest_cost(
    ctx: ExecutionContext,
    sdb: StoredDataBlockMetadata,
    target_storage: Storage,
    target_format: DataFormat,
) -> StoredDataBlockMetadata:
    target_storage_format = StorageFormat(target_storage.storage_type, target_format)
    cp = get_conversion_path_for_sdb(sdb, target_storage_format, ctx.all_storages)
    if cp is None:
        raise ConversionPathDoesNotExist(
            f"Converting {sdb} to {target_storage} {target_format}"
        )
    return convert_sdb(ctx, sdb, cp)


def get_conversion_path_for_sdb(
    sdb: StoredDataBlockMetadata,
    target_format: StorageFormat,
    storages: List[Storage],
) -> Optional[ConversionPath]:
    source_format = StorageFormat(sdb.storage.storage_type, sdb.data_format)
    if source_format == target_format:
        # Already exists, do nothing
        return ConversionPath()
    conversion = (source_format, target_format)
    conversion_path = get_converter_lookup().get_lowest_cost_path(
        conversion,
        storages=storages,
    )
    return conversion_path


def convert_sdb(
    ctx: ExecutionContext,
    sdb: StoredDataBlockMetadata,
    conversion_path: ConversionPath,
) -> StoredDataBlockMetadata:
    next_sdb = sdb
    for conversion_edge in conversion_path.conversions:
        conversion = conversion_edge.conversion
        target_storage_format = conversion[1]
        storage = select_storage(
            ctx.local_memory_storage, ctx.storages, target_storage_format
        )
        logger.debug(f"CONVERSION: {conversion[0]} -> {conversion[1]}")
        # printd("\t", storage)
        # printd("\t", next_sdb)
        next_sdb = conversion_edge.converter_class(ctx).convert(
            next_sdb, storage, target_storage_format.data_format
        )
    return next_sdb


def get_converter(
    sdb: StoredDataBlockMetadata,
    output_storage: Storage,
    output_format: DataFormat,
) -> Type[Converter]:
    target_format = StorageFormat(output_storage.storage_type, output_format)
    source_format = StorageFormat(sdb.storage.storage_type, sdb.data_format)
    conversion = (source_format, target_format)
    converter_class = get_converter_lookup().get_lowest_cost(conversion)
    if not converter_class:
        raise NotImplementedError(
            f"No converter to {target_format} from {source_format} for {sdb}"
        )
    return converter_class


def select_storage(
    local_memory_storage: Storage,
    storages: List[Storage],
    storage_format: StorageFormat,
) -> Storage:
    stype = storage_format.storage_type
    if stype == StorageType.DICT_MEMORY:
        return local_memory_storage
    for storage in storages:
        if stype == storage.storage_type:
            return storage
    raise Exception(f"No matching storage {storage_format}")
