from __future__ import annotations

import traceback
from collections import abc, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from io import IOBase
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional, Set

import sqlalchemy
from loguru import logger
from snapflow.core.data_block import (
    Alias,
    DataBlock,
    DataBlockMetadata,
    ManagedDataBlock,
    StoredDataBlockMetadata,
    create_data_block_from_records,
)
from snapflow.core.environment import Environment
from snapflow.core.metadata.orm import BaseModel
from snapflow.core.node import DataBlockLog, Direction, Node, SnapLog, get_state
from snapflow.core.runtime import Runtime, RuntimeClass, RuntimeEngine
from snapflow.core.snap import DataInterfaceType, InputExhaustedException, _Snap
from snapflow.core.snap_interface import (
    BoundInterface,
    NodeInterfaceManager,
    StreamInput,
)
from snapflow.core.storage import copy_lowest_cost
from snapflow.schema.base import Schema
from snapflow.storage.data_formats import DataFrameIterator, RecordsIterator
from snapflow.storage.data_formats.base import DataFormat, SampleableIterator
from snapflow.storage.data_records import (
    MemoryDataRecords,
    as_records,
    is_records_generator,
    records_object_is_definitely_empty,
    wrap_records_object,
)
from snapflow.storage.storage import LocalPythonStorageEngine, PythonStorageApi, Storage
from snapflow.utils.common import cf, error_symbol, success_symbol, utcnow
from snapflow.utils.data import SampleableIO
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from snapflow.core.graph import Graph, GraphMetadata


class Language(Enum):
    PYTHON = "python"
    SQL = "sql"


class LanguageDialect(Enum):
    CPYTHON = (Language.PYTHON, "cpython")
    POSTGRES = (Language.SQL, "postgres")
    MYSQL = (Language.SQL, "mysql")


INDENT = " " * 4
#
# @dataclass
# class StateManager:
#     """
#     Track Node state, so we can rollback on failure
#     """
#
#     state: Dict[str, Any]
#
#     def set(self, new_state: Dict[str, Any]):
#         self.state = new_state
#
#     def get(self):
#         return self.state


@dataclass(frozen=True)
class LanguageDialectSupport:
    language_dialect: LanguageDialect
    version_support: str  # e.g. >3.6.3,<4.0


@dataclass(frozen=True)
class CompiledSnap:  # TODO: this is unused currently, just a dumb wrapper on the snap
    key: str
    # code: str
    snap: _Snap  # TODO: compile this to actual string code we can run aaaaa-nnnnnnyyy-wheeeerrrrre
    # language_support: Iterable[LanguageDialect] = None  # TODO


@dataclass(frozen=True)
class RuntimeSpecification:
    runtime_class: RuntimeClass
    runtime_engine: RuntimeEngine
    runtime_version_requirement: str
    package_requirements: List[str]  # "extensions" for postgres, etc


# TODO: Idea is to have this be easily serializable to pass around to workers, along with RunContext
#      (Or subset RunConfiguration?). We're not really close to that yet...
@dataclass
class Executable:
    node_key: str
    compiled_snap: CompiledSnap
    # runtime_specification: RuntimeSpecification # TODO: support this
    bound_interface: BoundInterface = None
    params: Dict = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionSession:
    snap_log: SnapLog
    metadata_session: Session  # Make this a URL or other jsonable and then runtime can connect

    def log(self, block: DataBlockMetadata, direction: Direction):
        drl = DataBlockLog(  # type: ignore
            snap_log=self.snap_log,
            data_block=block,
            direction=direction,
            processed_at=utcnow(),
        )
        self.metadata_session.add(drl)

    def log_input(self, block: DataBlockMetadata):
        logger.debug(f"Input logged: {block}")
        self.log(block, Direction.INPUT)

    def log_output(self, block: DataBlockMetadata):
        logger.debug(f"Output logged: {block}")
        self.log(block, Direction.OUTPUT)


@dataclass
class ExecutionResult:
    inputs_bound: List[str]
    input_blocks_processed: Dict[str, int]
    output_block_count: int
    output_blocks_record_count: Optional[int] = None
    output_block_id: Optional[str] = None
    output_stored_block_id: Optional[str] = None
    output_alias: Optional[str] = None

    @classmethod
    def empty(cls) -> ExecutionResult:
        return ExecutionResult(
            inputs_bound=[], input_blocks_processed={}, output_block_count=0
        )


class ImproperlyStoredDataBlockException(Exception):
    pass


def validate_data_blocks(sess: Session):
    # TODO: More checks?
    sess.flush()
    for obj in sess.identity_map.values():
        if isinstance(obj, DataBlockMetadata):
            urls = set([sdb.storage_url for sdb in obj.stored_data_blocks])
            if all(u.startswith("python") for u in urls):
                fmts = set([sdb.data_format for sdb in obj.stored_data_blocks])
                if all(not f.is_storable() for f in fmts):
                    raise ImproperlyStoredDataBlockException(
                        f"DataBlock {obj} is not properly stored (no storeable format(s): {fmts})"
                    )


@dataclass  # (frozen=True)
class RunContext:
    env: Environment
    graph: Graph
    # metadata_session: Session
    storages: List[Storage]
    runtimes: List[Runtime]
    target_storage: Storage
    local_python_storage: Storage
    current_runtime: Optional[Runtime] = None
    node_timelimit_seconds: Optional[
        int
    ] = None  # TODO: this is a "soft" limit, could imagine a "hard" one too
    execution_timelimit_seconds: Optional[int] = None
    logger: Callable[[str], None] = lambda s: print(s, end="")

    def clone(self, **kwargs):
        args = dict(
            graph=self.graph,
            env=self.env,
            # metadata_session=self.metadata_session,
            storages=self.storages,
            runtimes=self.runtimes,
            target_storage=self.target_storage,
            local_python_storage=self.local_python_storage,
            current_runtime=self.current_runtime,
            node_timelimit_seconds=self.node_timelimit_seconds,
            execution_timelimit_seconds=self.execution_timelimit_seconds,
            logger=self.logger,
        )
        args.update(**kwargs)
        return RunContext(**args)  # type: ignore

    @contextmanager
    def start_snap_run(self, node: Node) -> Iterator[ExecutionSession]:
        from snapflow.core.graph import GraphMetadata

        assert self.current_runtime is not None, "Runtime not set"
        with self.env.session_scope() as sess:
            node_state_obj = node.get_state(sess)
            if node_state_obj is None:
                node_state = {}
            else:
                node_state = node_state_obj.state
            new_graph_meta = node.graph.get_metadata_obj()
            graph_meta = sess.query(GraphMetadata).get(new_graph_meta.hash)
            if graph_meta is None:
                sess.add(new_graph_meta)
                sess.flush([new_graph_meta])
                graph_meta = new_graph_meta

            pl = SnapLog(  # type: ignore
                graph_id=graph_meta.hash,
                node_key=node.key,
                node_start_state={k: v for k, v in node_state.items()},
                node_end_state=node_state,
                snap_key=node.snap.key,
                snap_params=node.params,
                runtime_url=self.current_runtime.url,
                started_at=utcnow(),
            )

            try:
                yield ExecutionSession(pl, sess)
                # Validate local memory objects: Did we leave any non-storeables hanging?
                validate_data_blocks(sess)
            except Exception as e:
                logger.debug(f"Error running node:\n{traceback.format_exc()}")
                pl.set_error(e)
                # raise e
            finally:
                # Persist state on success OR error:
                pl.persist_state(sess)
                pl.completed_at = utcnow()
                sess.add(pl)
                sess.flush()

    @property
    def all_storages(self) -> List[Storage]:
        # TODO: should it be in the list of storages already?
        return [self.local_python_storage] + self.storages

    # def to_json(self) -> str:
    #     return json.dumps(
    #         dict(
    #             env=self.env,
    #             storages=self.storages,
    #             runtimes=self.runtimes,
    #             target_storage=self.target_storage,
    #         ),
    #         cls=SnapflowJSONEncoder,
    #     )
    #
    # @classmethod
    # def from_json(cls, dct: Dict) -> ExecutionContext:
    #     return ExecutionContext(
    #         env=Environment.from_json(dct["env"]),
    #         storages=dct["storages"],
    #         runtimes=dct["runtimes"],
    #         target_storage=dct["target_storage"],
    #     )


@dataclass(frozen=True)
class SnapContext:  # TODO: (Generic[C, S]):
    run_context: RunContext
    execution_session: ExecutionSession
    worker: Worker
    executable: Executable
    inputs: List[StreamInput]
    snap_log: SnapLog
    input_blocks_processed: Dict[str, Set[DataBlockMetadata]] = field(
        default_factory=lambda: defaultdict(set)
    )
    outputs: List[StoredDataBlockMetadata] = field(default_factory=list)
    # state: Dict = field(default_factory=dict)
    # emitted_states: List[Dict] = field(default_factory=list)
    # resolved_output_schema: Optional[Schema] = None
    # realized_output_schema: Optional[Schema]

    # def get_resolved_output_schema(self) -> Optional[Schema]:
    #     return self.execution.bound_interface.resolved_output_schema(
    #         self.execution_context.env
    #     )
    #
    # def set_resolved_output_schema(self, schema: Schema):
    #     self.execution.bound_interface.set_resolved_output_schema(schema)
    #
    # def set_output_schema(self, schema_like: SchemaLike):
    #     if not schema_like:
    #         return
    #     schema = self.execution_context.env.get_schema(schema_like)
    #     self.set_resolved_output_schema(schema)

    def get_node(self) -> Node:
        return self.run_context.graph.get_node(self.executable.node_key)

    def get_param(self, key: str, default: Any = None) -> Any:
        return self.executable.params.get(key, default)

    def get_params(self) -> Dict[str, Any]:
        return self.executable.params

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
        data_format: DataFormat = None,
        schema: Schema = None,
        update_state: Dict[str, Any] = None,
        replace_state: Dict[str, Any] = None,
    ):
        if records_obj is not None:
            sdb = self.handle_records_object(
                records_obj, data_format=data_format, schema=schema
            )
            if sdb is not None:
                self.create_alias(sdb)
                self.execution_session.log_output(sdb.data_block)
                self.outputs.append(sdb)
        if update_state is not None:
            for k, v in update_state.items():
                self.emit_state_value(k, v)
        if replace_state is not None:
            self.emit_state(replace_state)
        # Commit input blocks to db as well, to save progress
        self.log_input_blocks()

    def handle_records_object(
        self,
        records_obj: Any = None,
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
            nominal_output_schema = self.executable.bound_interface.resolve_nominal_output_schema(
                self.run_context.env,
                self.execution_session.metadata_session,
            )  # TODO: could check output to see if it is LocalRecords with a schema too?
        logger.debug(
            f"Resolved output schema {nominal_output_schema} {self.executable.bound_interface}"
        )
        records_obj = wrap_records_object(records_obj)
        if records_object_is_definitely_empty(records_obj):
            # TODO
            # Are we sure we'd never want to process an empty object?
            # Like maybe create the db table, but leave it empty? could be useful
            return None
        dro = as_records(
            records_obj, data_format=data_format, schema=nominal_output_schema
        )
        sdb = self.store_output_block(dro)
        return sdb

    def create_alias(self, sdb: StoredDataBlockMetadata) -> Optional[Alias]:
        self.execution_session.metadata_session.flush([sdb.data_block, sdb])
        alias = ensure_alias(
            self.execution_session.metadata_session, self.get_node(), sdb
        )
        self.execution_session.metadata_session.flush([alias])
        return alias

    def store_output_block(self, dro: MemoryDataRecords) -> StoredDataBlockMetadata:
        block, sdb = create_data_block_from_records(
            self.run_context.env,
            self.execution_session.metadata_session,
            self.run_context.local_python_storage,
            dro,
            created_by_node_key=self.executable.node_key,
        )
        # TODO: need target_format option too
        if (
            self.run_context.target_storage is None
            or self.run_context.target_storage == sdb.storage
        ):
            # Already good on target storage
            if sdb.data_format.is_storable():
                # And its storable
                return sdb

        # check if existing storage_format is compatible with target storage,
        # and it's storable, then use instead of natural (no need to convert)
        target_format = (
            self.run_context.target_storage.storage_engine.get_natural_format()
        )
        if self.run_context.target_storage.storage_engine.is_supported_format(
            sdb.data_format
        ):
            if sdb.data_format.is_storable():
                target_format = sdb.data_format

        assert target_format.is_storable()

        # Place output in target storage
        return copy_lowest_cost(
            self.run_context.env,
            self.execution_session.metadata_session,
            sdb=sdb,
            target_storage=self.run_context.target_storage,
            target_format=target_format,
            eligible_storages=self.run_context.storages,
        )

    def log_input_blocks(self):
        for input in self.executable.bound_interface.inputs:
            if input.bound_stream is not None:
                for db in input.bound_stream.get_emitted_blocks():
                    self.input_blocks_processed[input.name].add(db)
                    self.execution_session.log_input(db)

    def should_continue(self) -> bool:
        """
        Long running snaps should check this function periodically so
        as to honor time limits.
        """
        # TODO: execution timelimit too?
        #   Since long running will often be generators, could also enforce this at generator evaluation time?
        if not self.run_context.node_timelimit_seconds:
            return True
        seconds_elapsed = (utcnow() - self.snap_log.started_at).total_seconds()
        return seconds_elapsed < self.run_context.node_timelimit_seconds


class ExecutionManager:
    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self.env = ctx.env

    def select_runtime(self, node: Node) -> Runtime:
        compatible_runtimes = node.snap.compatible_runtime_classes
        for runtime in self.ctx.runtimes:
            if (
                runtime.runtime_engine.runtime_class in compatible_runtimes
            ):  # TODO: Just taking the first one...
                return runtime
        raise Exception(
            f"No compatible runtime available for {node} (runtime class {compatible_runtimes} required)"
        )

    def execute(
        self,
        node: Node,
        to_exhaustion: bool = False,
        output_session: Optional[Session] = None,
    ) -> Optional[DataBlock]:
        runtime = self.select_runtime(node)
        run_ctx = self.ctx.clone(current_runtime=runtime)
        worker = Worker(run_ctx)

        # Setup for run
        base_msg = f"Running node {cf.bold(node.key)} {cf.dimmed(node.snap.key)}\n"
        self.ctx.logger(base_msg)
        logger.debug(
            f"RUNNING NODE {node.key} {node.snap.key} with params `{node.params}`"
        )
        # self.log(base_msg)
        # start = time.time()
        n_runs: int = 0
        last_execution_result: Optional[ExecutionResult] = None
        last_non_none_output: Optional[str] = None
        try:
            while True:
                last_execution_result = self._execute(node, worker)
                if last_execution_result.output_block_id is not None:
                    last_non_none_output = last_execution_result.output_block_id
                n_runs += 1
                if (
                    not to_exhaustion or not last_execution_result.inputs_bound
                ):  # TODO: We just run no-input DFs (source extractors) once no matter what
                    # (they are responsible for creating their own generators)
                    break
            self.log_execution_result(last_execution_result)
            self.ctx.logger(INDENT + cf.success("Ok " + success_symbol + "\n"))  # type: ignore
        except InputExhaustedException as e:  # TODO: i don't think we need this out here anymore (now that extractors don't throw)
            logger.debug(INDENT + cf.warning("Input Exhausted"))
            if e.args:
                logger.debug(e)
            if n_runs == 0:
                self.ctx.logger(INDENT + "Inputs: No unprocessed upstream\n")
            self.ctx.logger(INDENT + cf.success("Ok " + success_symbol + "\n"))  # type: ignore
        except Exception as e:
            self.ctx.logger(INDENT + cf.error("Error " + error_symbol) + str(e) + "\n")  # type: ignore
            raise e

        # TODO: how to pass back with session?
        #   maybe with env.produce() as output:
        # Or just merge in new session in env.produce
        if last_non_none_output is None:
            return None
        logger.debug(f"*DONE* RUNNING NODE {node.key} {node.snap.key}")
        if output_session is not None:
            db: DataBlockMetadata = output_session.query(DataBlockMetadata).get(
                last_non_none_output
            )
            return db.as_managed_data_block(run_ctx, output_session)
        return None

    def _execute(self, node: Node, worker: Worker) -> ExecutionResult:
        snap = node.snap
        executable = Executable(
            node_key=node.key,
            compiled_snap=CompiledSnap(
                key=node.key,
                snap=snap,
            ),
            # bound_interface=interface_mgr.get_bound_interface(),
            params=node.params or {},
        )
        return worker.execute(executable)

    def log_execution_result(self, result: ExecutionResult):
        self.ctx.logger(INDENT + "Inputs: ")
        if result.input_blocks_processed:
            self.ctx.logger("\n")
            for input_name, cnt in result.input_blocks_processed.items():
                self.ctx.logger(
                    INDENT * 2 + f"{input_name}: {cnt} block(s) processed\n"
                )
        else:
            self.ctx.logger("None\n")
        self.ctx.logger(INDENT + f"Outputs: {result.output_block_count} blocks")
        if result.output_blocks_record_count:
            self.ctx.logger(f" ({result.output_blocks_record_count} records)")
        if result.output_block_id is not None:
            self.ctx.logger(
                f" Alias: {result.output_alias if result.output_alias is not None else '-'} "
                + cf.dimmed(f"({result.output_block_id})")  # type: ignore
            )
        self.ctx.logger("\n")


def ensure_alias(sess: Session, node: Node, sdb: StoredDataBlockMetadata) -> Alias:
    logger.debug(
        f"Creating alias {node.get_alias()} for node {node.key} on storage {sdb.storage_url}"
    )
    return sdb.create_alias(sess, node.get_alias())


class Worker:
    def __init__(self, ctx: RunContext):
        self.env = ctx.env
        self.ctx = ctx

    def execute(self, executable: Executable) -> ExecutionResult:
        node = self.ctx.graph.get_node(executable.node_key)
        result = ExecutionResult.empty()
        with self.ctx.start_snap_run(node) as execution_session:
            interface_mgr = NodeInterfaceManager(
                self.ctx, execution_session.metadata_session, node
            )
            executable.bound_interface = interface_mgr.get_bound_interface()
            snap_ctx = SnapContext(
                self.ctx,
                worker=self,
                execution_session=execution_session,
                executable=executable,
                inputs=executable.bound_interface.inputs,
                snap_log=execution_session.snap_log,
            )
            snap_args = []
            if executable.bound_interface.context:
                snap_args.append(snap_ctx)
            snap_inputs = executable.bound_interface.inputs_as_kwargs()
            snap_kwargs = snap_inputs
            # Actually run the snap
            output_obj = executable.compiled_snap.snap.snap_callable(
                *snap_args, **snap_kwargs
            )
            for res in self.process_execution_result(
                executable, execution_session, output_obj, snap_ctx
            ):
                result = res
        logger.debug(f"EXECUTION RESULT {result}")
        return result

    def process_execution_result(
        self,
        executable: Executable,
        execution_session: ExecutionSession,
        output_obj: DataInterfaceType,
        snap_ctx: SnapContext,
    ) -> Iterator[ExecutionResult]:
        result = ExecutionResult.empty()
        if output_obj is not None:
            if is_records_generator(output_obj):
                output_iterator = output_obj
            else:
                output_iterator = [output_obj]
            i = 0
            for output_obj in output_iterator:
                logger.debug(output_obj)
                i += 1
                snap_ctx.emit(output_obj)
                result = self.execution_result_info(
                    executable, execution_session, snap_ctx
                )
                yield result

    def execution_result_info(
        self,
        executable: Executable,
        execution_session: ExecutionSession,
        snap_ctx: SnapContext,
    ) -> ExecutionResult:
        last_output_block: Optional[DataBlockMetadata] = None
        last_output_sdb: Optional[StoredDataBlockMetadata] = None
        last_output_alias: Optional[Alias] = None

        # Flush
        # execution_session.metadata_session.flush()

        if snap_ctx.outputs:
            last_output_sdb = snap_ctx.outputs[-1]
            last_output_block = last_output_sdb.data_block
            last_output_alias = last_output_sdb.get_alias(
                execution_session.metadata_session
            )

        input_block_counts = {}
        total_input_count = 0
        for input_name, dbs in snap_ctx.input_blocks_processed.items():
            input_block_counts[input_name] = len(dbs)
            total_input_count += len(dbs)

        return ExecutionResult(
            inputs_bound=list(executable.bound_interface.inputs_as_kwargs().keys()),
            input_blocks_processed=input_block_counts,
            output_block_id=last_output_block.id,
            output_stored_block_id=last_output_sdb.id,
            output_alias=last_output_alias.alias,
            output_block_count=len(snap_ctx.outputs),
            output_blocks_record_count=sum(
                [
                    db.record_count()
                    for db in snap_ctx.outputs
                    if db.record_count() is not None
                ]
            ),
        )

    # TODO: where does this sql stuff really belong? Are we still using this?
    def get_connection(self) -> sqlalchemy.engine.Engine:
        if self.ctx.current_runtime is None:
            raise Exception("Current runtime not set")
        if self.ctx.current_runtime.runtime_class != RuntimeClass.DATABASE:
            raise Exception(f"Runtime not supported {self.ctx.current_runtime}")
        return sqlalchemy.create_engine(self.ctx.current_runtime.url)

    def execute_sql(self, sql: str) -> ResultProxy:
        logger.debug("Executing SQL:")
        logger.debug(sql)
        return self.get_connection().execute(sql)
