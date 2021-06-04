from snapflow.core.persisted.state import DataBlockLog, DataFunctionLog, NodeState
from typing import List
from snapflow.core.persisted.data_block import (
    Alias,
    DataBlockMetadata,
    StoredDataBlockMetadata,
)
from pydantic_sqlalchemy.main import sqlalchemy_to_pydantic


DataBlockMetadataCfg = sqlalchemy_to_pydantic(DataBlockMetadata)
StoredDataBlockMetadataCfg = sqlalchemy_to_pydantic(StoredDataBlockMetadata)
AliasCfg = sqlalchemy_to_pydantic(Alias)


class DataBlockWithStoredBlocksCfg(DataBlockMetadataCfg):
    stored_data_blocks: List[StoredDataBlockMetadataCfg] = []


DataFunctionLogCfg = sqlalchemy_to_pydantic(DataFunctionLog)
DataBlockLogCfg = sqlalchemy_to_pydantic(DataBlockLog)
NodeStateCfg = sqlalchemy_to_pydantic(NodeState)
