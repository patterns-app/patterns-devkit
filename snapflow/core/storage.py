from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Type

from dcp import StorageFormat
from dcp.data_copy.base import CopyRequest
from dcp.data_copy.graph import execute_copy_request, get_copy_path
from dcp.data_format.base import DataFormat
from dcp.storage.base import Storage
from loguru import logger
from snapflow.core.data_block import (
    DataBlockMetadata,
    StoredDataBlockMetadata,
    get_stored_datablock_id,
)
from snapflow.core.environment import Environment
from sqlalchemy import or_
from sqlalchemy.sql.expression import select


def copy_sdb(
    env: Environment,
    request: CopyRequest,
    in_sdb: StoredDataBlockMetadata,
    out_sdb: StoredDataBlockMetadata,
    # target_storage: Storage,
    # storages: Optional[List[Storage]] = None,
    create_intermediate_sdbs: bool = True,
):
    result = execute_copy_request(request)
    if create_intermediate_sdbs:
        for name, storage, fmt in result.intermediate_created:
            i_sdb = StoredDataBlockMetadata(  # type: ignore
                id=get_stored_datablock_id(),
                data_block_id=in_sdb.data_block_id,
                data_block=in_sdb.data_block,
                data_format=fmt,
                storage_url=storage.url,
            )
            storage.get_api().create_alias(name, i_sdb.get_name_for_storage())
            env.md_api.add(i_sdb)


def ensure_data_block_on_storage(
    env: Environment,
    block: DataBlockMetadata,
    storage: Storage,
    fmt: Optional[DataFormat] = None,
    eligible_storages: Optional[List[Storage]] = None,
) -> StoredDataBlockMetadata:
    if eligible_storages is None:
        eligible_storages = env.storages
    sdbs = select(StoredDataBlockMetadata).filter(
        StoredDataBlockMetadata.data_block == block
    )
    match = sdbs.filter(StoredDataBlockMetadata.storage_url == storage.url)
    if fmt:
        match = match.filter(StoredDataBlockMetadata.data_format == fmt)
    matched_sdb = env.md_api.execute(match).scalar_one_or_none()
    if matched_sdb is not None:
        return matched_sdb

    # logger.debug(f"{cnt} SDBs total")
    existing_sdbs = sdbs.filter(
        # DO NOT fetch memory SDBs that aren't of current runtime (since we can't get them!)
        # TODO: clean up memory SDBs when the memory goes away? Doesn't make sense to persist them really
        # Should be a separate in-memory lookup for memory SDBs, so they naturally expire?
        or_(
            ~StoredDataBlockMetadata.storage_url.startswith("python:"),
            StoredDataBlockMetadata.storage_url == env._local_python_storage.url,
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
    existing_sdbs = list(env.md_api.execute(existing_sdbs).scalars())
    for sdb in existing_sdbs:
        req = CopyRequest(
            from_name=sdb.get_name_for_storage(),
            from_storage=sdb.storage,
            to_name="placeholder",
            to_storage=storage,
            to_format=fmt,
            schema=sdb.realized_schema(env),
            available_storages=eligible_storages,
        )
        pth = get_copy_path(req)
        if pth is not None:
            eligible_conversion_paths.append((pth.total_cost, pth, sdb, req))
    if not eligible_conversion_paths:
        raise NotImplementedError(
            f"No copy path to {target_storage_format} for existing StoredDataBlocks {existing_sdbs}"
        )
    cost, pth, in_sdb, req = min(eligible_conversion_paths, key=lambda x: x[0])
    out_sdb = StoredDataBlockMetadata(  # type: ignore
        id=get_stored_datablock_id(),
        data_block_id=block.id,
        data_block=block,
        data_format=fmt,
        storage_url=storage.url,
    )
    env.md_api.add(out_sdb)
    req.to_name = out_sdb.get_name_for_storage()
    copy_sdb(
        env,
        request=req,
        in_sdb=in_sdb,
        out_sdb=out_sdb,
    )
    return out_sdb
