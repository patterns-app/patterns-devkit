from __future__ import annotations
from snapflow.core.environment import Environment
from snapflow.core.execution.executable import (
    Executable,
    ExecutionContext,
    ExecutionResult,
)

import traceback
from collections import abc, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from io import IOBase
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)
import datacopy
from datacopy.data_format.base import DataFormat
from datacopy.utils.common import rand_str, utcnow
from openmodel.base import Schema

import sqlalchemy
from loguru import logger
from snapflow.core.data_block import (
    Alias,
    DataBlockMetadata,
    ManagedDataBlock,
    StoredDataBlockMetadata,
    get_datablock_id,
)
from snapflow.core.metadata.api import MetadataApi
from snapflow.core.node import DataBlockLog, Direction, Node, SnapLog, get_state
from snapflow.core.snap import DataInterfaceType, InputExhaustedException, _Snap
from snapflow.core.snap_interface import (
    BoundInterface,
    NodeInterfaceManager,
    StreamInput,
)
from sqlalchemy.sql.expression import select
from snapflow.utils.output import cf, success_symbol, error_symbol


class ImproperlyStoredDataBlockException(Exception):
    pass


def validate_data_blocks(env: Environment):
    # TODO: More checks?
    env.md_api.flush()
    for obj in env.md_api.active_session.identity_map.values():
        if isinstance(obj, DataBlockMetadata):
            urls = set([sdb.storage_url for sdb in obj.stored_data_blocks])
            if all(u.startswith("python") for u in urls):
                fmts = set([sdb.data_format for sdb in obj.stored_data_blocks])
                if all(not f.is_storable() for f in fmts):
                    raise ImproperlyStoredDataBlockException(
                        f"DataBlock {obj} is not properly stored (no storeable format(s): {fmts})"
                    )


DEFAULT_OUTPUT_NAME = "default"


@dataclass(frozen=True)
class SnapContext:  # TODO: (Generic[C, S]):
    env: Environment
    snap: _Snap
    node: Node
    executable: Executable
    metadata_api: MetadataApi
    inputs: List[StreamInput]
    bound_interface: BoundInterface
    snap_log: SnapLog
    execution_context: ExecutionContext
    input_blocks_processed: Dict[str, Set[DataBlockMetadata]] = field(
        default_factory=lambda: defaultdict(set)
    )
    output_blocks_emitted: Dict[str, StoredDataBlockMetadata] = field(
        default_factory=dict
    )

    def ensure_log(self, block: DataBlockMetadata, direction: Direction, name: str):
        if self.metadata_api.execute(
            select(DataBlockLog).filter(
                snap_log=self.snap_log,
                stream_name=name,
                data_block=block,
                direction=direction,
            )
        ).exists():
            return
        drl = DataBlockLog(  # type: ignore
            snap_log=self.snap_log,
            stream_name=name,
            data_block=block,
            direction=direction,
            processed_at=utcnow(),
        )
        self.metadata_api.add(drl)

    def log_all(self):
        for input_name, blocks in self.input_blocks_processed.items():
            for block in blocks:
                self.ensure_log(block, Direction.INPUT, input_name)
                logger.debug(f"Input logged: {block}")
        for output_name, block in self.output_blocks_emitted.items():
            logger.debug(f"Output logged: {block}")
            self.ensure_log(block, Direction.OUTPUT, output_name)

    def get_param(self, key: str, default: Any = None) -> Any:
        if default is None:
            try:
                default = self.snap.get_param(key).default
            except KeyError:
                pass
        return self.node.params.get(key, default)

    def get_params(self, defaults: Dict[str, Any] = None) -> Dict[str, Any]:
        final_params = {p.name: p.default for p in self.snap.params}
        final_params.update(defaults or {})
        final_params.update(self.node.params)
        return final_params

    def get_state_value(self, key: str, default: Any = None) -> Any:
        assert isinstance(self.snap_log.node_end_state, dict)
        return self.snap_log.node_end_state.get(key, default)

    def get_state(self) -> Dict[str, Any]:
        return self.snap_log.node_end_state

    def emit_state_value(self, key: str, new_value: Any):
        new_state = self.snap_log.node_end_state.copy()
        new_state[key] = new_value
        self.snap_log.node_end_state = new_state

    def emit_state(self, new_state: Dict):
        self.snap_log.node_end_state = new_state

    def emit(
        self,
        records_obj: Any = None,
        output: str = DEFAULT_OUTPUT_NAME,
        data_format: DataFormat = None,
        schema: Schema = None,
        update_state: Dict[str, Any] = None,
        replace_state: Dict[str, Any] = None,
    ):
        if records_obj is not None:
            sdb = self.handle_records_object(
                records_obj, output, data_format=data_format, schema=schema
            )
            if sdb is not None:
                self.create_alias(sdb)
        if update_state is not None:
            for k, v in update_state.items():
                self.emit_state_value(k, v)
        if replace_state is not None:
            self.emit_state(replace_state)
        # Commit input blocks to db as well, to save progress
        self.log_processed_input_blocks()

    def create_alias(self, sdb: StoredDataBlockMetadata) -> Optional[Alias]:
        self.metadata_api.flush()  # [sdb.data_block, sdb])
        alias = sdb.create_alias(self.env, self.node.get_alias())
        self.metadata_api.flush()  # [alias])
        return alias

    def create_stored_datablock(self) -> StoredDataBlockMetadata:
        block = DataBlockMetadata(
            id=get_datablock_id(),
            inferred_schema_key=None,
            nominal_schema_key=None,
            realized_schema_key="Any",
            record_count=None,
            created_by_node_key=self.node.key,
        )
        sdb = StoredDataBlockMetadata(  # type: ignore
            data_block_id=block.id,
            data_block=block,
            storage_url=self.execution_context.local_storage.url,
            data_format=None,
        )
        return sdb

    def get_stored_datablock_for_output(self, output: str) -> StoredDataBlockMetadata:
        sdb = self.output_blocks_emitted.get(output)
        if sdb is None:
            self.output_blocks_emitted[output] = self.create_stored_datablock()
            return self.get_stored_datablock_for_output(output)
        return sdb

    def handle_records_object(
        self,
        records_obj: Any,
        output: str,
        data_format: DataFormat = None,
        schema: Schema = None,
    ) -> Optional[StoredDataBlockMetadata]:
        logger.debug(f"HANDLING EMITTED OBJECT (of type {type(records_obj)})")
        # TODO: can i return an existing DataBlock? Or do I need to create a "clone"?
        #   Answer: ok to return as is (just mark it as 'output' in DBL)
        if isinstance(records_obj, StoredDataBlockMetadata):
            # TODO is it in local storage tho? we skip conversion below...
            # This is just special case right now to support SQL snap
            # Will need better solution for explicitly creating DB/SDBs inside of snaps
            return records_obj
        elif isinstance(records_obj, DataBlockMetadata):
            raise NotImplementedError
        elif isinstance(records_obj, ManagedDataBlock):
            raise NotImplementedError
        nominal_output_schema = schema
        if nominal_output_schema is None:
            nominal_output_schema = self.bound_interface.resolve_nominal_output_schema(
                self.env
            )  # TODO: could check output to see if it is LocalRecords with a schema too?
        logger.debug(
            f"Resolved output schema {nominal_output_schema} {self.bound_interface}"
        )
        sdb = self.get_stored_datablock_for_output(output)
        self.append_records_to_stored_datablock(records_obj, sdb)
        # self.executable.execution_context.local_storage.get_api().count(tmp_name)
        # if records_object_is_definitely_empty(records_obj):
        #     # TODO
        #     # Are we sure we'd never want to process an empty object?
        #     # Like maybe create the db table, but leave it empty? could be useful
        #     return None
        # sdb = self.store_output_block(dro)
        return sdb

    def append_records_to_stored_datablock(
        self, records_obj: Any, sdb: StoredDataBlockMetadata
    ):
        # TODO: infer incoming schema, check against existing schema on sdb
        tmp_name = "_tmp_obj_" + rand_str(10)
        self.execution_context.local_storage.get_api().put(tmp_name, records_obj)
        # fmt = infer_format_for_name(tmp_name, self.executable.execution_context.local_storage)
        # TODO: to_format
        # TODO: make sure this handles no-ops (empty object, same storage)
        datacopy.copy(
            from_name=tmp_name,
            from_storage=self.execution_context.local_storage,
            to_name=sdb.datablock.get_name_for_storage(),
            to_storage=sdb.storage,
            available_storages=self.execution_context.storages,
            exists="append",
        )
        self.execution_context.local_storage.get_api().remove_alias(
            tmp_name
        )  # TODO: actual remove method? equivalent for python local storage

    # def store_output_block(self, dro: MemoryDataRecords) -> StoredDataBlockMetadata:
    #     block, sdb = create_data_block_from_records(
    #         self.run_context.env,
    #         self.executable.execution_context.local_storage,
    #         dro,
    #         created_by_node_key=self.executable.node_key,
    #     )
    #     # TODO: need target_format option too
    #     if (
    #         self.run_context.target_storage is None
    #         or self.run_context.target_storage == sdb.storage
    #     ):
    #         # Already good on target storage
    #         if sdb.data_format.is_storable():
    #             # And its storable
    #             return sdb

    #     # check if existing storage_format is compatible with target storage,
    #     # and it's storable, then use instead of natural (no need to convert)
    #     target_format = (
    #         self.run_context.target_storage.storage_engine.get_natural_format()
    #     )
    #     if self.run_context.target_storage.storage_engine.is_supported_format(
    #         sdb.data_format
    #     ):
    #         if sdb.data_format.is_storable():
    #             target_format = sdb.data_format

    #     assert target_format.is_storable()

    #     # Place output in target storage
    #     return copy_lowest_cost(
    #         self.run_context.env,
    #         sdb=sdb,
    #         target_storage=self.run_context.target_storage,
    #         target_format=target_format,
    #         eligible_storages=self.run_context.storages,
    #     )

    def log_processed_input_blocks(self):
        for input in self.bound_interface.inputs:
            if input.bound_stream is not None:
                for db in input.bound_stream.get_emitted_blocks():
                    self.input_blocks_processed[input.name].add(db)

    def should_continue(self) -> bool:
        """
        Long running snaps should check this function periodically
        to honor time limits.
        """
        if not self.execution_context.execution_timelimit_seconds:
            return True
        seconds_elapsed = (utcnow() - self.snap_log.started_at).total_seconds()
        return seconds_elapsed < self.execution_context.execution_timelimit_seconds

    def as_execution_result(self) -> ExecutionResult:
        input_block_counts = {}
        for input_name, dbs in self.input_blocks_processed.items():
            input_block_counts[input_name] = len(dbs)
        output_blocks = {}
        for output_name, sdb in self.output_blocks_emitted.items():
            output_blocks[output_name] = {
                "id": sdb.data_block_id,
                "record_count": sdb.record_count,
                "alias": sdb.get_alias(self.env),
            }
        return ExecutionResult(
            inputs_bound=list(self.bound_interface.inputs_as_kwargs().keys()),
            non_reference_inputs_bound=self.bound_interface.non_reference_bound_inputs(),
            input_block_counts=input_block_counts,
            output_blocks=output_blocks,
            error=self.snap_log.error.get("error")
            if isinstance(self.snap_log.error, dict)
            else None,
            traceback=self.snap_log.error.get("traceback")
            if isinstance(self.snap_log.error, dict)
            else None,
        )


class ExecutionManager:
    def __init__(self, exe: Executable):
        self.exe = exe
        self.env = exe.execution_context.env
        self.logger = exe.execution_context.logger
        self.node = self.exe.node

    def execute(self) -> ExecutionResult:
        # Setup for run
        base_msg = (
            f"Running node {cf.bold(self.node.key)} {cf.dimmed(self.node.snap.key)}\n"
        )
        logger.debug(
            f"RUNNING NODE {self.node.key} {self.node.snap.key} with params `{self.node.params}`"
        )
        self.logger.log(base_msg)
        with self.logger.indent():
            try:
                result = self._execute()
                self.log_execution_result(result)
            # except InputExhaustedException as e:
            #     logger.debug(cf.warning("Input Exhausted"))
            #     if e.args:
            #         logger.debug(e)
            #     self.logger.log("Inputs: No unprocessed inputs\n")
            except Exception as e:
                raise e
            if not result.error:
                self.logger.log(cf.success("Ok " + success_symbol + "\n"))  # type: ignore
            else:
                error = result.error or "Snap failed (unknown error)"
                self.logger.log(cf.error("Error " + error_symbol + " " + cf.dimmed(error[:80])) + "\n")  # type: ignore
                if result.traceback:
                    self.logger.log(cf.dimmed(result.traceback), indent=8)  # type: ignore
            logger.debug(f"Execution result: {result}")
            logger.debug(f"*DONE* RUNNING NODE {self.node.key} {self.node.snap.key}")
        return result

    def _execute(self) -> ExecutionResult:
        result = ExecutionResult.empty()
        try:
            with self.start_snap_run(self.node) as snap_log:
                interface_mgr = NodeInterfaceManager(self.exe)
                bound_interface = interface_mgr.get_bound_interface()
                snap_ctx = SnapContext(
                    env=self.env,
                    snap=self.exe.snap,
                    node=self.exe.node,
                    executable=self.exe,
                    metadata_api=self.env.md_api,
                    inputs=bound_interface.inputs,
                    snap_log=snap_log,
                    bound_interface=bound_interface,
                    execution_context=self.exe.execution_context,
                )
                snap_args = []
                if snap_ctx.bound_interface.context:
                    snap_args.append(snap_ctx)
                snap_inputs = snap_ctx.bound_interface.inputs_as_kwargs()
                snap_kwargs = snap_inputs
                # TODO: tighten up the contextmanager to around just this call?
                #       Otherwise we are catching framework errors as snap errors
                try:
                    # snap = executable.compiled_snap.snap
                    # local_vars = locals()
                    # if hasattr(snap, "_locals"):
                    #     local_vars.update(snap._locals)
                    # exec(snap.get_source_code(), globals(), local_vars)
                    # output_obj = local_vars[snap.snap_callable.__name__](
                    output_obj = snap_ctx.snap.snap_callable(*snap_args, **snap_kwargs,)
                    if output_obj is not None:
                        self.emit_output_object(output_obj, snap_ctx)
                finally:
                    pass
                result = snap_ctx.as_execution_result()
        except Exception as e:
            result.set_error(e)
            if self.exe.execution_context.raise_on_snap_error:
                raise e

        logger.debug(f"EXECUTION RESULT {result}")
        return result

    def emit_output_object(
        self, output_obj: DataInterfaceType, snap_ctx: SnapContext,
    ):
        assert output_obj is not None
        if isinstance(output_obj, abc.Generator):
            output_iterator = output_obj
        else:
            output_iterator = [output_obj]
        i = 0
        for output_obj in output_iterator:
            logger.debug(output_obj)
            i += 1
            snap_ctx.emit(output_obj)

    @contextmanager
    def start_snap_run(self, node: Node) -> Iterator[SnapLog]:
        from snapflow.core.graph import GraphMetadata

        # assert self.current_runtime is not None, "Runtime not set"
        md = self.env.get_metadata_api()
        with md.begin():
            node_state_obj = node.get_state()
            if node_state_obj is None:
                node_state = {}
            else:
                node_state = node_state_obj.state
            new_graph_meta = node.graph.get_metadata_obj()
            graph_meta = md.execute(
                select(GraphMetadata).filter(GraphMetadata.hash == new_graph_meta.hash)
            ).scalar_one_or_none()
            if graph_meta is None:
                md.add(new_graph_meta)
                md.flush()  # [new_graph_meta])
                graph_meta = new_graph_meta

            pl = SnapLog(  # type: ignore
                graph_id=graph_meta.hash,
                node_key=node.key,
                node_start_state=node_state.copy(),  # {k: v for k, v in node_state.items()},
                node_end_state=node_state,
                snap_key=node.snap.key,
                snap_params=node.params,
                # runtime_url=self.current_runtime.url,
                started_at=utcnow(),
            )

            try:
                yield pl
                # Validate local memory objects: Did we leave any non-storeables hanging?
                validate_data_blocks(self.env)
            except Exception as e:
                # Don't worry about exhaustion exceptions
                if not isinstance(e, InputExhaustedException):
                    logger.debug(f"Error running node:\n{traceback.format_exc()}")
                    pl.set_error(e)
                    pl.persist_state(self.env)
                    pl.completed_at = utcnow()
                    md.add(pl)
                    # TODO: should clean this up so transaction surrounds things that you DO
                    #       want to rollback, obviously
                    md.commit()  # MUST commit here since the re-raised exception will issue a rollback
                    # Re-raise here and handle elsewhere
                    raise e
            finally:
                # Persist state on success OR error:
                pl.persist_state(self.env)
                pl.completed_at = utcnow()
                md.add(pl)

    def log_execution_result(self, result: ExecutionResult):
        self.logger.log("Inputs: ")
        if result.input_block_counts:
            self.logger.log("\n")
            with self.logger.indent():
                for input_name, cnt in result.input_block_counts.items():
                    self.logger.log(f"{input_name}: {cnt} block(s) processed\n")
        else:
            self.logger.log("None\n")
        self.logger.log("Outputs: ")
        if result.output_blocks:
            self.logger.log("\n")
            with self.logger.indent():
                for output_name, block_summary in result.output_blocks.items():
                    self.logger.log(f"{output_name}:")
                    cnt = block_summary["record_count"]
                    alias = block_summary["alias"]
                    if cnt is not None:
                        self.logger.log(f"{cnt} records")
                    self.logger.log(
                        f"{alias}" + cf.dimmed(f"({block_id})\n")  # type: ignore
                    )
        else:
            self.logger.log("None\n")
        self.logger.log("\n")
