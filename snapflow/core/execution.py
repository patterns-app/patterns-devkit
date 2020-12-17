from __future__ import annotations

from collections import abc
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from io import IOBase
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional

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
from snapflow.core.node import DataBlockLog, Direction, Node, PipeLog, get_state
from snapflow.core.pipe import DataInterfaceType, InputExhaustedException, Pipe
from snapflow.core.pipe_interface import (
    BoundInterface,
    NodeInterfaceManager,
    StreamInput,
)
from snapflow.core.runtime import Runtime, RuntimeClass, RuntimeEngine
from snapflow.core.storage import copy_lowest_cost
from snapflow.storage.data_formats import DataFrameIterator, RecordsIterator
from snapflow.storage.data_formats.base import SampleableIterator
from snapflow.storage.data_records import (
    as_records,
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
class CompiledPipe:  # TODO: this is unused currently, just a dumb wrapper on the pipe
    key: str
    # code: str
    pipe: Pipe  # TODO: compile this to actual string code we can run aaaaa-nnnnnnyyy-wheeeerrrrre
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
    compiled_pipe: CompiledPipe
    # runtime_specification: RuntimeSpecification # TODO: support this
    bound_interface: BoundInterface = None
    configuration: Dict = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionSession:
    pipe_log: PipeLog
    metadata_session: Session  # Make this a URL or other jsonable and then runtime can connect

    def log(self, block: DataBlockMetadata, direction: Direction):
        drl = DataBlockLog(  # type: ignore
            pipe_log=self.pipe_log,
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
    output_block: Optional[DataBlockMetadata] = None
    output_stored_block: Optional[StoredDataBlockMetadata] = None


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
    def start_pipe_run(self, node: Node) -> Iterator[ExecutionSession]:
        from snapflow.core.graph import GraphMetadata

        assert self.current_runtime is not None, "Runtime not set"
        with self.env.session_scope() as sess:
            node_state = node.get_state(sess) or {}
            new_graph_meta = node.graph.get_metadata_obj()
            graph_meta = sess.query(GraphMetadata).get(new_graph_meta.hash)
            if graph_meta is None:
                sess.add(new_graph_meta)
                sess.flush([new_graph_meta])
                graph_meta = new_graph_meta

            pl = PipeLog(  # type: ignore
                graph_id=graph_meta.hash,
                node_key=node.key,
                node_start_state=node_state,
                node_end_state=node_state,
                pipe_key=node.pipe.key,
                pipe_config=node.config,
                runtime_url=self.current_runtime.url,
                started_at=utcnow(),
            )

            try:
                yield ExecutionSession(pl, sess)
                # Validate local memory objects: Did we leave any non-storeables hanging?
                validate_data_blocks(sess)
                # Only persist state on successful run
                pl.persist_state(sess)

            except Exception as e:
                pl.set_error(e)
                raise e
            finally:
                pl.completed_at = utcnow()
                sess.add(pl)

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
class PipeContext:  # TODO: (Generic[C, S]):
    run_context: RunContext
    execution_session: ExecutionSession
    worker: Worker
    executable: Executable
    inputs: List[StreamInput]
    pipe_log: PipeLog
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

    def get_config_value(self, key: str, default: Any = None) -> Any:
        return self.executable.configuration.get(key, default)

    def get_config(self) -> Dict[str, Any]:
        return self.executable.configuration

    def get_state_value(self, key: str, default: Any = None) -> Any:
        assert isinstance(self.pipe_log.node_end_state, dict)
        return self.pipe_log.node_end_state.get(key, default)

    def get_state(self) -> Dict[str, Any]:
        return self.pipe_log.node_end_state

    def emit_state_value(self, key: str, new_value: Any):
        new_state = self.pipe_log.node_end_state.copy()
        new_state[key] = new_value
        self.pipe_log.node_end_state = new_state

    def emit_state(self, new_state: Dict):
        self.pipe_log.node_end_state = new_state

    def should_continue(self) -> bool:
        """
        Long running pipes should check this function periodically so
        as to honor time limits.
        """
        # TODO: execution timelimit too?
        #   Since long running will often be generators, could also enforce this at generator evaluation time?
        if not self.run_context.node_timelimit_seconds:
            return True
        seconds_elapsed = (utcnow() - self.pipe_log.started_at).total_seconds()
        return seconds_elapsed < self.run_context.node_timelimit_seconds


class ExecutionManager:
    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self.env = ctx.env

    def select_runtime(self, node: Node) -> Runtime:
        compatible_runtimes = node.pipe.compatible_runtime_classes
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
        base_msg = f"Running node {cf.bold(node.key)} {cf.dimmed(node.pipe.key)}\n"
        self.ctx.logger(base_msg)
        logger.debug(
            f"RUNNING NODE {node.key} {node.pipe.key} with config `{node.config}`"
        )
        # self.log(base_msg)
        # start = time.time()
        n_runs: int = 0
        last_execution_result: Optional[ExecutionResult] = None
        last_non_none_output: Optional[DataBlockMetadata] = None
        try:
            while True:
                last_execution_result = self._execute(node, worker)
                if last_execution_result.output_block is not None:
                    last_non_none_output = last_execution_result.output_block
                n_runs += 1
                if (
                    not to_exhaustion or not last_execution_result.inputs_bound
                ):  # TODO: We just run no-input DFs (source extractors) once no matter what
                    # (they are responsible for creating their own generators)
                    break
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
        logger.debug(f"*DONE* RUNNING NODE {node.key} {node.pipe.key}")
        if output_session is not None:
            last_non_none_output = output_session.merge(last_non_none_output)
            return last_non_none_output.as_managed_data_block(run_ctx, output_session)
        return None

    def _execute(self, node: Node, worker: Worker) -> ExecutionResult:
        pipe = node.pipe
        executable = Executable(
            node_key=node.key,
            compiled_pipe=CompiledPipe(
                key=node.key,
                pipe=pipe,
            ),
            # bound_interface=interface_mgr.get_bound_interface(),
            configuration=node.config or {},
        )
        return worker.execute(executable)


def ensure_alias(sess: Session, node: Node, sdb: StoredDataBlockMetadata) -> Alias:
    logger.debug(
        f"Creating alias {node.get_alias()} for node {node.key} on storage {sdb.storage_url}"
    )
    return sdb.create_alias(node.env, sess, node.get_alias())


class Worker:
    def __init__(self, ctx: RunContext):
        self.env = ctx.env
        self.ctx = ctx

    def execute(self, executable: Executable) -> ExecutionResult:
        node = self.ctx.graph.get_node(executable.node_key)
        with self.ctx.start_pipe_run(node) as execution_session:
            interface_mgr = NodeInterfaceManager(
                self.ctx, execution_session.metadata_session, node
            )
            executable.bound_interface = interface_mgr.get_bound_interface()
            pipe_ctx = PipeContext(
                self.ctx,
                worker=self,
                execution_session=execution_session,
                executable=executable,
                inputs=executable.bound_interface.inputs,
                pipe_log=execution_session.pipe_log,
            )
            pipe_args = []
            if executable.bound_interface.requires_pipe_context:
                pipe_args.append(pipe_ctx)
            pipe_inputs = executable.bound_interface.inputs_as_kwargs()
            pipe_kwargs = pipe_inputs
            # Actually run the pipe
            output_obj = executable.compiled_pipe.pipe.pipe_callable(
                *pipe_args, **pipe_kwargs
            )
            result = self.process_execution_result(
                executable, execution_session, output_obj
            )
        return result

    def process_execution_result(
        self,
        executable: Executable,
        execution_session: ExecutionSession,
        output_obj: DataInterfaceType,
    ) -> ExecutionResult:
        node = self.ctx.graph.get_node(executable.node_key)
        output_block: Optional[DataBlockMetadata] = None
        output_sdb: Optional[StoredDataBlockMetadata] = None
        if output_obj is not None:
            output_sdb = self.handle_raw_output_object(
                execution_session, output_obj, executable
            )
            alias = None
            # Output block may be none still if `output` was an empty generator
            if output_sdb is not None:
                execution_session.metadata_session.flush(
                    [output_sdb.data_block, output_sdb]
                )
                output_block = output_sdb.data_block
                execution_session.log_output(output_block)
                alias = ensure_alias(
                    execution_session.metadata_session, node, output_sdb
                )
                execution_session.metadata_session.flush([alias])

        # Flush
        execution_session.metadata_session.flush()

        input_block_counts = {}
        total_input_count = 0
        for input in executable.bound_interface.inputs:
            if input.bound_stream is not None:
                input_block_counts[input.name] = 0
                for db in input.bound_stream.get_emitted_blocks():
                    input_block_counts[input.name] += 1
                    total_input_count += 1
                    execution_session.log_input(db)
        # Log
        if executable.bound_interface.inputs:
            self.ctx.logger(
                INDENT + f"Inputs: {total_input_count} block(s) processed\n"
            )
        if output_block is not None:
            self.ctx.logger(
                INDENT
                + f"Output: {alias.alias} "
                + cf.dimmed(
                    f"({output_block.id}) {output_block.record_count} records"
                )  # type: ignore
                + "\n"
            )

        return ExecutionResult(
            inputs_bound=list(executable.bound_interface.inputs_as_kwargs().keys()),
            input_blocks_processed=input_block_counts,
            output_block=output_block,
            output_stored_block=output_sdb,
        )

    def handle_raw_output_object(
        self,
        execution_session: ExecutionSession,
        output_obj: DataInterfaceType,
        execution: Executable,
    ) -> Optional[StoredDataBlockMetadata]:
        logger.debug("HANDLING OUTPUT")
        # TODO: can i return an existing DataBlock? Or do I need to create a "clone"?
        #   Answer: ok to return as is (just mark it as 'output' in DBL)
        if isinstance(output_obj, StoredDataBlockMetadata):
            # TODO is it in local storage tho? we skip conversion below...
            # This is just special case right now to support SQL pipe
            # Will need better solution for explicitly creating DB/SDBs inside of pipes
            return output_obj
        elif isinstance(output_obj, DataBlockMetadata):
            raise NotImplementedError
        elif isinstance(output_obj, ManagedDataBlock):
            raise NotImplementedError
        else:
            # TODO: handle DataBlock stream output (iterator that goes into separate blocks)
            nominal_output_schema = execution.bound_interface.resolve_nominal_output_schema(
                self.env,
                execution_session.metadata_session,
            )  # TODO: could check output to see if it is LocalRecords with a schema too?
            logger.debug(
                f"Resolved output schema {nominal_output_schema} {execution.bound_interface}"
            )
            output_obj = wrap_records_object(output_obj)
            if records_object_is_definitely_empty(output_obj):
                # TODO
                # Are we sure we'd never want to process an empty object?
                # Like maybe create the db table, but leave it empty? could be useful
                return None
            dro = as_records(output_obj, schema=nominal_output_schema)
            block, sdb = create_data_block_from_records(
                self.env,
                execution_session.metadata_session,
                self.ctx.local_python_storage,
                dro,
                created_by_node_key=execution.node_key,
            )

        # TODO: need target_format option too
        if self.ctx.target_storage is None or self.ctx.target_storage == sdb.storage:
            # Already good on target storage
            if sdb.data_format.is_storable():
                # And its storable
                return sdb

        # check if existing storage_format is compatible with target storage,
        # and it's storable, then use instead of natural (no need to convert)
        target_format = self.ctx.target_storage.storage_engine.get_natural_format()
        if self.ctx.target_storage.storage_engine.is_supported_format(sdb.data_format):
            if sdb.data_format.is_storable():
                target_format = sdb.data_format

        assert target_format.is_storable()

        # Place output in target storage
        return copy_lowest_cost(
            self.ctx.env,
            execution_session.metadata_session,
            sdb=sdb,
            target_storage=self.ctx.target_storage,
            target_format=target_format,
            eligible_storages=self.ctx.storages,
        )

    # TODO: where does this sql stuff really belong?
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
