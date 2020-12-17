from loguru import logger
from sqlalchemy import event
from sqlalchemy.orm import object_session, sessionmaker

# def add_persisting_sdb_listener(session_maker: sessionmaker):
#     @event.listens_for(session_maker, "pending_to_persistent", propagate=True)
#     def intercept_t2p(session, instance):
#         from snapflow.core.data_block import StoredDataBlockMetadata

#         if isinstance(instance, StoredDataBlockMetadata):
#             logger.debug(
#                 f"Persisted StoredDataBlock SDB#{cf.bold(instance.id)} DB#{cf.bold(instance.data_block.id)} {cf.magenta(instance.data_block.expected_schema_key)} {cf.dimmed_magenta(instance.data_format)}"
#             )


class ImmutableObjectException(Exception):
    pass


def immutability_update_listener(mapper, connection, target):
    if object_session(target).is_modified(target, include_collections=False):
        raise ImmutableObjectException("DataBlocks and StoredDataBlocks are immutable")


# @event.listens_for(BaseModel, "init", propagate=True)
# def intercept_init(instance, args, kwargs):
#     from snapflow.core.data_block import StoredDataBlock
#
#     if isinstance(instance, StoredDataBlock):
#         print(instance)
#         print(type(instance))
#         schema = getattr(instance.data_block, "schema", None)
#         print(
#             f"New transient StoredDataBlock {cf.magenta(schema)} {cf.dimmed_magenta(instance.data_format)}"
#         )
# print(f"{cf.orange}New transient:{cf.reset} {instance!r}")
