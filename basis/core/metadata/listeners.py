from sqlalchemy import event
from sqlalchemy.orm import object_session, sessionmaker

from basis.utils.common import cf, printd


def add_persisting_sdb_listener(session_maker: sessionmaker):
    @event.listens_for(session_maker, "pending_to_persistent", propagate=True)
    def intercept_t2p(session, instance):
        from basis.core.data_block import StoredDataBlockMetadata

        if isinstance(instance, StoredDataBlockMetadata):
            printd(
                f"Persisted StoredDataBlock SDR#{cf.bold(instance.id)} DR#{cf.bold(instance.data_block.id)} {cf.magenta(instance.data_block.declared_otype_uri)} {cf.dimmed_magenta(instance.data_format)}"
            )


def immutability_update_listener(mapper, connection, target):
    if object_session(target).is_modified(target, include_collections=False):
        raise Exception("DRs and SDRs are immutable")  # TODO exception cleanup


# @event.listens_for(BaseModel, "init", propagate=True)
# def intercept_init(instance, args, kwargs):
#     from basis.core.data_block import StoredDataBlock
#
#     if isinstance(instance, StoredDataBlock):
#         print(instance)
#         print(type(instance))
#         otype = getattr(instance.data_block, "otype", None)
#         print(
#             f"New transient StoredDataBlock {cf.magenta(otype)} {cf.dimmed_magenta(instance.data_format)}"
#         )
# print(f"{cf.orange}New transient:{cf.reset} {instance!r}")
