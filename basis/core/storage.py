from __future__ import annotations

import os
from typing import TYPE_CHECKING, List, Optional, Type

from basis.core.component import ComponentLibrary, global_library
from basis.core.environment import Environment
from basis.core.persistence.block import make_sdb_name
from basis.core.persistence.pydantic import (
    BlockMetadataCfg,
    StoredBlockMetadataCfg,
)
from dcp import StorageFormat
from dcp.data_copy.base import CopyRequest
from dcp.data_copy.graph import execute_copy_request, get_copy_path
from dcp.data_format.base import DataFormat
from dcp.storage.base import Storage
from loguru import logger
from sqlalchemy import or_
from sqlalchemy.sql.expression import select


def copy_sdb_cfg(
    request: CopyRequest,
    in_sdb: StoredBlockMetadataCfg,
    out_sdb: StoredBlockMetadataCfg,
    # target_storage: Storage,
    # storages: Optional[List[Storage]] = None,
    create_intermediate_sdbs: bool = True,
) -> List[StoredBlockMetadataCfg]:
    stored_blocks = []
    result = execute_copy_request(request)
    if create_intermediate_sdbs:
        for name, storage, fmt in result.intermediate_created:
            sid = get_stored_block_id()
            i_sdb = StoredBlockMetadataCfg(  # type: ignore
                id=sid,
                name=make_sdb_name(sid, in_sdb.block.created_by_node_key),
                block=in_sdb.block,
                data_format=fmt,
                storage_url=storage.url,
                data_is_written=True,
            )
            storage.get_api().create_alias(name, i_sdb.name)
            stored_blocks.append(i_sdb)
    return stored_blocks


def ensure_block_on_storage_cfg(
    block: BlockMetadataCfg,
    storage: Storage,
    stored_blocks: List[StoredBlockMetadataCfg],
    eligible_storages: List[Storage],
    fmt: Optional[DataFormat] = None,
    library: ComponentLibrary = global_library,
) -> List[StoredBlockMetadataCfg]:
    sdbs = stored_blocks
    match = [s for s in sdbs if s.storage.url == storage.url]
    if fmt:
        match = [s for s in match if s.data_format == fmt]
    if match:
        return [match[0]]

    fmt = fmt or storage.storage_engine.get_natural_format()
    target_storage_format = StorageFormat(storage.storage_engine, fmt)

    # Compute conversion costs
    eligible_conversion_paths = (
        []
    )  #: List[List[Tuple[ConversionCostLevel, Type[Converter]]]] = []
    existing_sdbs = sdbs
    for sdb in existing_sdbs:
        assert sdb.name is not None
        req = CopyRequest(
            from_name=sdb.name,
            from_storage=sdb.storage,
            to_name="placeholder",
            to_storage=storage,
            to_format=fmt,
            schema=library.get_schema(block.realized_schema_key),
            available_storages=eligible_storages,
        )
        pth = get_copy_path(req)
        if pth is not None:
            eligible_conversion_paths.append((pth.total_cost, pth, sdb, req))
    if not eligible_conversion_paths:
        raise NotImplementedError(
            f"No copy path to {target_storage_format} for existing StoredBlocks {existing_sdbs}"
        )
    cost, pth, in_sdb, req = min(eligible_conversion_paths, key=lambda x: x[0])
    sid = get_stored_block_id()
    out_sdb = StoredBlockMetadataCfg(  # type: ignore
        id=sid,
        name=make_sdb_name(sid, block.created_by_node_key),
        block_id=block.id,
        block=block,
        data_format=fmt.nickname,
        storage_url=storage.url,
        data_is_written=True,
    )
    req.to_name = out_sdb.name
    created_sdbs = copy_sdb_cfg(request=req, in_sdb=in_sdb, out_sdb=out_sdb,)
    return [out_sdb] + created_sdbs
