from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Type

from loguru import logger
from snapflow.core.data_block import (
    DataBlockMetadata,
    StoredDataBlockMetadata,
    get_datablock_id,
)
from snapflow.core.environment import Environment
from snapflow.storage.data_copy.base import (
    Conversion,
    ConversionPath,
    StorageFormat,
    get_datacopy_lookup,
)
from snapflow.storage.data_formats import DataFormat
from snapflow.storage.storage import LocalPythonStorageEngine, Storage
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, event, or_
from sqlalchemy.orm.session import Session

if TYPE_CHECKING:
    from snapflow.core.execution import RunContext


class CopyPathDoesNotExist(Exception):
    pass


def copy_lowest_cost(
    env: Environment,
    sess: Session,
    sdb: StoredDataBlockMetadata,
    target_storage: Storage,
    target_format: DataFormat,
    eligible_storages: Optional[List[Storage]] = None,
) -> StoredDataBlockMetadata:
    if eligible_storages is None:
        eligible_storages = env.storages
    target_storage_format = StorageFormat(target_storage.storage_engine, target_format)
    cp = get_copy_path_for_sdb(sdb, target_storage_format, eligible_storages)
    if cp is None:
        raise CopyPathDoesNotExist(
            f"Copying {sdb} to format {target_format} on storage {target_storage}"
        )
    return convert_sdb(
        env,
        sess=sess,
        sdb=sdb,
        conversion_path=cp,
        target_storage=target_storage,
        storages=eligible_storages,
    )


def get_copy_path_for_sdb(
    sdb: StoredDataBlockMetadata, target_format: StorageFormat, storages: List[Storage]
) -> Optional[ConversionPath]:
    source_format = StorageFormat(sdb.storage.storage_engine, sdb.data_format)
    if source_format == target_format:
        # Already exists, do nothing
        return ConversionPath()
    conversion = Conversion(source_format, target_format)
    conversion_path = get_datacopy_lookup(
        available_storage_engines=set(s.storage_engine for s in storages),
    ).get_lowest_cost_path(
        conversion,
    )
    return conversion_path


def convert_sdb(
    env: Environment,
    sess: Session,
    sdb: StoredDataBlockMetadata,
    conversion_path: ConversionPath,
    target_storage: Storage,
    storages: Optional[List[Storage]] = None,
) -> StoredDataBlockMetadata:
    if not conversion_path.conversions:
        return sdb
    if storages is None:
        storages = env.storages
    prev_sdb = sdb
    next_sdb: Optional[StoredDataBlockMetadata] = None
    prev_storage = sdb.storage
    next_storage: Optional[Storage] = None
    realized_schema = sdb.realized_schema(env, sess)
    for conversion_edge in conversion_path.conversions:
        conversion = conversion_edge.conversion
        target_storage_format = conversion.to_storage_format
        next_storage = select_storage(target_storage, storages, target_storage_format)
        logger.debug(
            f"CONVERSION: {conversion.from_storage_format} -> {conversion.to_storage_format}"
        )
        next_sdb = StoredDataBlockMetadata(  # type: ignore
            id=get_datablock_id(),
            data_block_id=prev_sdb.data_block_id,
            data_block=prev_sdb.data_block,
            data_format=target_storage_format.data_format,
            storage_url=next_storage.url,
        )
        sess.add(next_sdb)
        conversion_edge.copier.copy(
            from_name=prev_sdb.get_name(),
            to_name=next_sdb.get_name(),
            conversion=conversion,
            from_storage_api=prev_storage.get_api(),
            to_storage_api=next_storage.get_api(),
            schema=realized_schema,
        )
        if (
            prev_sdb.data_format.is_python_format()
            and not prev_sdb.data_format.is_storable()
        ):
            # If the records obj is in python and not storable, and we just used it, then it can be reused
            # TODO: Bit of a hack. Is there a central place we can do this?
            #       also is reusable a better name than storable?
            prev_storage.get_api().remove(prev_sdb.get_name())
            prev_sdb.data_block.stored_data_blocks.remove(prev_sdb)
            if prev_sdb in sess.new:
                sess.expunge(prev_sdb)
            else:
                sess.delete(prev_sdb)
        prev_sdb = next_sdb
        prev_storage = next_storage
    return next_sdb


def ensure_data_block_on_storage(
    env: Environment,
    sess: Session,
    block: DataBlockMetadata,
    storage: Storage,
    fmt: Optional[DataFormat] = None,
    eligible_storages: Optional[List[Storage]] = None,
) -> StoredDataBlockMetadata:
    if eligible_storages is None:
        eligible_storages = env.storages
    sdbs = sess.query(StoredDataBlockMetadata).filter(
        StoredDataBlockMetadata.data_block == block
    )
    match = sdbs.filter(StoredDataBlockMetadata.storage_url == storage.url)
    if fmt:
        match = match.filter(StoredDataBlockMetadata.data_format == fmt)
    matched_sdb = match.first()
    if matched_sdb is not None:
        return matched_sdb

    # logger.debug(f"{cnt} SDBs total")
    existing_sdbs = sdbs.filter(
        # DO NOT fetch memory SDBs that aren't of current runtime (since we can't get them!)
        # TODO: clean up memory SDBs when the memory goes away? Doesn't make sense to persist them really
        # Should be a separate in-memory lookup for memory SDBs, so they naturally expire?
        or_(
            ~StoredDataBlockMetadata.storage_url.startswith("python:"),
            StoredDataBlockMetadata.storage_url == storage.url,
        ),
    )
    # logger.debug(
    #     f"{existing_sdbs.count()} SDBs on-disk or in local memory (local: {self.ctx.local_python_storage.url})"
    # )
    if eligible_storages:
        existing_sdbs = existing_sdbs.filter(
            StoredDataBlockMetadata.storage_url.in_(s.url for s in eligible_storages),
        )
    # logger.debug(f"{existing_sdbs.count()} SDBs in eligible storages")
    fmt = fmt or storage.storage_engine.get_natural_format()
    target_storage_format = StorageFormat(storage.storage_engine, fmt)

    # Compute conversion costs
    eligible_conversion_paths = (
        []
    )  #: List[List[Tuple[ConversionCostLevel, Type[Converter]]]] = []
    existing_sdbs = list(existing_sdbs)
    for sdb in existing_sdbs:
        conversion_path = get_copy_path_for_sdb(
            sdb, target_storage_format, eligible_storages
        )
        if conversion_path is not None:
            eligible_conversion_paths.append(
                (conversion_path.total_cost, conversion_path, sdb)
            )
    if not eligible_conversion_paths:
        raise NotImplementedError(
            f"No converter to {target_storage_format} for existing StoredDataBlocks {existing_sdbs}"
        )
    cost, conversion_path, in_sdb = min(eligible_conversion_paths, key=lambda x: x[0])
    return convert_sdb(
        env,
        sess=sess,
        sdb=in_sdb,
        conversion_path=conversion_path,
        target_storage=storage,
        storages=eligible_storages,
    )


def select_storage(
    target_storage: Storage,
    storages: List[Storage],
    storage_format: StorageFormat,
) -> Storage:
    eng = storage_format.storage_engine
    if eng == target_storage.storage_engine:
        return target_storage
    for storage in storages:
        if eng == storage.storage_engine:
            return storage
    raise Exception(f"No matching storage {storage_format}")
