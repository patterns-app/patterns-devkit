from __future__ import annotations

from collections import abc
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional

import sqlalchemy
from loguru import logger
from snapflow.core.conversion import convert_lowest_cost
from snapflow.core.data_block import (
    Alias,
    DataBlock,
    DataBlockMetadata,
    ManagedDataBlock,
    StoredDataBlockMetadata,
    create_data_block_from_records,
)
from snapflow.core.data_formats import DataFrameGenerator, RecordsListGenerator
from snapflow.core.data_formats.base import ReusableGenerator
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
from snapflow.core.storage.storage import LocalMemoryStorageEngine, Storage
from snapflow.utils.common import cf, error_symbol, success_symbol, utcnow
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


@dataclass(frozen=True)
class Runnable:
    node_key: str
    compiled_pipe: CompiledPipe
    # runtime_specification: RuntimeSpecification # TODO: support this
    bound_interface: BoundInterface
    configuration: Dict = field(default_factory=dict)


@dataclass(frozen=True)
class RunSession:
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
class RunResult:
    inputs_bound: List[str]
    input_blocks_processed: Dict[str, int]
    output_block: Optional[DataBlockMetadata] = None
    output_stored_block: Optional[StoredDataBlockMetadata] = None


@dataclass  # (frozen=True)
class ExecutionContext:
    env: Environment
    graph: Graph
    metadata_session: Session
    storages: List[Storage]
    runtimes: List[Runtime]
    target_storage: Storage
    local_memory_storage: Storage
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
            metadata_session=self.metadata_session,
            storages=self.storages,
            runtimes=self.runtimes,
            target_storage=self.target_storage,
            local_memory_storage=self.local_memory_storage,
            current_runtime=self.current_runtime,
            node_timelimit_seconds=self.node_timelimit_seconds,
            execution_timelimit_seconds=self.execution_timelimit_seconds,
            logger=self.logger,
        )
        args.update(**kwargs)
        return ExecutionContext(**args)  # type: ignore

    @contextmanager
    def start_pipe_run(self, node: Node) -> Iterator[RunSession]:
        from snapflow.core.graph import GraphMetadata

        assert self.current_runtime is not None, "Runtime not set"
        node_state = node.get_state(self.metadata_session) or {}
        new_graph_meta = node.graph.get_metadata_obj()
        graph_meta = self.metadata_session.query(GraphMetadata).get(new_graph_meta.hash)
        if graph_meta is None:
            graph_meta = self.metadata_session.merge(new_graph_meta)
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
            yield RunSession(pl, self.metadata_session)
            # Only persist state on successful run
            pl.persist_state(self.metadata_session)

        except Exception as e:
            pl.set_error(e)
            raise e
        finally:
            pl.completed_at = utcnow()
            self.metadata_session.add(pl)
            self.metadata_session.flush()

    def add(self, obj: BaseModel) -> BaseModel:
        try:
            self.metadata_session.add(obj)
        except InvalidRequestError as e:
            # Already in session perhaps
            logger.debug(f"Can't add obj {obj}: {e}")
        return self.merge(obj)

    def merge(self, obj: BaseModel) -> BaseModel:
        return self.metadata_session.merge(obj)

    @property
    def all_storages(self) -> List[Storage]:
        # TODO: should it be in the list of storages already?
        return [self.local_memory_storage] + self.storages

    # def to_json(self) -> str:
    #     return json.dumps(
    #         dict(
    #             env=self.env,
    #             storages=self.storages,
    #             runtimes=self.runtimes,
    #             target_storage=self.target_storage,
    #         ),
    #         cls=DagsJSONEncoder,
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
    execution_context: ExecutionContext
    worker: Worker
    runnable: Runnable
    inputs: List[StreamInput]
    pipe_log: PipeLog
    # state: Dict = field(default_factory=dict)
    # emitted_states: List[Dict] = field(default_factory=list)
    # resolved_output_schema: Optional[Schema] = None
    # realized_output_schema: Optional[Schema]

    # def get_resolved_output_schema(self) -> Optional[Schema]:
    #     return self.runnable.bound_interface.resolved_output_schema(
    #         self.execution_context.env
    #     )
    #
    # def set_resolved_output_schema(self, schema: Schema):
    #     self.runnable.bound_interface.set_resolved_output_schema(schema)
    #
    # def set_output_schema(self, schema_like: SchemaLike):
    #     if not schema_like:
    #         return
    #     schema = self.execution_context.env.get_schema(schema_like)
    #     self.set_resolved_output_schema(schema)

    def get_config_value(self, key: str, default: Any = None) -> Any:
        return self.runnable.configuration.get(key, default)

    def get_config(self) -> Dict[str, Any]:
        return self.runnable.configuration

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
        if not self.execution_context.node_timelimit_seconds:
            return True
        seconds_elapsed = (utcnow() - self.pipe_log.started_at).total_seconds()
        return seconds_elapsed < self.execution_context.node_timelimit_seconds


class ExecutionManager:
    def __init__(self, ctx: ExecutionContext):
        self.ctx = ctx
        self.env = ctx.env

    def select_runtime(self, node: Node) -> Runtime:
        compatible_runtimes = node.pipe.compatible_runtime_classes
        for runtime in self.ctx.runtimes:
            if (
                runtime.runtime_class in compatible_runtimes
            ):  # TODO: Just taking the first one...
                return runtime
        raise Exception(
            f"No compatible runtime available for {node} (runtime class {compatible_runtimes} required)"
        )

    def get_node_interface_manager(self, node: Node) -> NodeInterfaceManager:
        return NodeInterfaceManager(self.ctx, node)

    def run(self, node: Node, to_exhaustion: bool = False) -> Optional[DataBlock]:
        runtime = self.select_runtime(node)
        run_ctx = self.ctx.clone(current_runtime=runtime)
        worker = Worker(run_ctx)
        last_output: Optional[DataBlockMetadata] = None

        # Setup for run
        base_msg = f"Running node {cf.bold(node.key)} {cf.dimmed(node.pipe.key)}\n"
        self.ctx.logger(base_msg)
        logger.debug(
            f"RUNNING NODE {node.key} {node.pipe.key} with config `{node.config}`"
        )
        # self.log(base_msg)
        # start = time.time()
        n_runs = 0
        run_result = None
        try:
            while True:
                run_result = self._run(node, worker)
                n_runs += 1
                if (
                    not to_exhaustion or not run_result.inputs_bound
                ):  # TODO: We just run no-input DFs (source extractors) once no matter what
                    # (they are responsible for creating their own generators)
                    break
            self.ctx.logger(INDENT + cf.success("Ok " + success_symbol + "\n"))
        except InputExhaustedException as e:  # TODO: i don't think we need this out here anymore (now that extractors don't throw)
            logger.debug(INDENT + cf.warning("Input Exhausted"))
            if e.args:
                logger.debug(e)
            if n_runs == 0:
                self.ctx.logger(INDENT + "No unprocessed inputs\n")
            self.ctx.logger(INDENT + cf.success("Ok " + success_symbol + "\n"))
        except Exception as e:
            self.ctx.logger(INDENT + cf.error("Error " + error_symbol) + str(e) + "\n")
            raise e

        if run_result is None:
            return None
        last_output = run_result.output_block
        if last_output is None:
            return None
        last_output = self.env.session.merge(last_output)
        logger.debug(f"*DONE* RUNNING NODE {node.key} {node.pipe.key}")
        return last_output.as_managed_data_block(self.ctx)

    def _run(self, node: Node, worker: Worker) -> RunResult:
        interface_mgr = self.get_node_interface_manager(node)
        pipe = node.pipe
        runnable = Runnable(
            node_key=node.key,
            compiled_pipe=CompiledPipe(
                key=node.key,
                pipe=pipe,
            ),
            bound_interface=interface_mgr.get_bound_interface(),
            configuration=node.config or {},
        )
        return worker.run(runnable)


def ensure_alias(node: Node, sdb: StoredDataBlockMetadata) -> Alias:
    logger.debug(
        f"Creating alias {node.get_alias()} for node {node.key} on storage {sdb.storage_url}"
    )
    return sdb.create_alias(node.env, node.get_alias())


class Worker:
    def __init__(self, ctx: ExecutionContext):
        self.env = ctx.env
        self.ctx = ctx

    def run(self, runnable: Runnable) -> RunResult:
        output_block: Optional[DataBlockMetadata] = None
        node = self.ctx.graph.get_node(runnable.node_key)
        with self.ctx.start_pipe_run(node) as run_session:
            pipe_ctx = PipeContext(
                self.ctx,
                worker=self,
                runnable=runnable,
                inputs=runnable.bound_interface.inputs,
                pipe_log=run_session.pipe_log,
            )
            pipe_args = []
            if runnable.bound_interface.requires_pipe_context:
                pipe_args.append(pipe_ctx)
            pipe_inputs = runnable.bound_interface.inputs_as_kwargs()
            pipe_kwargs = pipe_inputs
            # Actually run the pipe
            output = runnable.compiled_pipe.pipe.pipe_callable(
                *pipe_args, **pipe_kwargs
            )
            output_block = None
            output_block = None
            output_sdb = None
            if output is not None:
                assert (
                    self.ctx.target_storage is not None
                ), "Must specify target storage for output"
                output_sdb = self.handle_output(run_session, output, runnable)
                alias = None
                # Output block may be none still if `output` was an empty generator
                if output_sdb is not None:
                    output_block = output_sdb.data_block
                    run_session.log_output(output_block)
                    alias = ensure_alias(node, output_sdb)
                    alias = self.ctx.merge(alias)

            input_block_counts = {}
            total_input_count = 0
            for input in runnable.bound_interface.inputs:
                if input.bound_stream is not None:
                    input_block_counts[input.name] = 0
                    for db in input.bound_stream.get_emitted_blocks():
                        input_block_counts[input.name] += 1
                        total_input_count += 1
                        run_session.log_input(db)
            # Log
            if runnable.bound_interface.inputs:
                self.ctx.logger(
                    INDENT + f"{total_input_count} input blocks processed\n"
                )
            if output_block is not None:
                self.ctx.logger(
                    INDENT
                    + f"Output block: ({alias.alias}) "
                    + cf.dimmed(str(output_block.id))
                    + "\n"
                )
        return RunResult(
            inputs_bound=list(pipe_inputs.keys()),
            input_blocks_processed=input_block_counts,
            output_block=output_block,
            output_stored_block=output_sdb,
        )

    def handle_output(
        self,
        worker_session: RunSession,
        output: DataInterfaceType,
        runnable: Runnable,
    ) -> Optional[StoredDataBlockMetadata]:
        logger.debug("HANDLING OUTPUT")
        assert runnable.bound_interface.output is not None
        # TODO: can i return an existing DataBlock? Or do I need to create a "clone"?
        #   Answer: ok to return as is (just mark it as 'output' in DBL)
        assert self.ctx.target_storage is not None
        if isinstance(output, StoredDataBlockMetadata):
            output = self.ctx.merge(output)
            # TODO is it in local storage tho? we skip conversion below...
            return output
        elif isinstance(output, DataBlockMetadata):
            raise NotImplementedError
        elif isinstance(output, ManagedDataBlock):
            raise NotImplementedError
        else:
            # TODO: handle generic generator "Generator" (or call it "Stream" or "OutputStream"?)
            if isinstance(output, abc.Generator):
                if (
                    runnable.bound_interface.output.data_format_class
                    == "DataFrameGenerator"
                ):
                    output = DataFrameGenerator(output)
                elif (
                    runnable.bound_interface.output.data_format_class
                    == "RecordsListGenerator"
                ):
                    output = RecordsListGenerator(output)
                else:
                    output = ReusableGenerator(output)
                if output.get_one() is None:
                    # Empty generator
                    return None
            nominal_output_schema = runnable.bound_interface.resolve_nominal_output_schema(
                self.env
            )  # TODO: could check output to see if it is LocalRecords with a schema too?
            logger.debug(
                f"Resolved output schema {nominal_output_schema} {runnable.bound_interface}"
            )
            block, sdb = create_data_block_from_records(
                self.env,
                self.ctx.local_memory_storage,
                output,
                nominal_schema=nominal_output_schema,
                created_by_node_key=runnable.node_key,
            )

        # TODO: check if existing storage_format is compatible with target storage, instead of using natural (no need to convert then)
        # Place output in target storage
        return convert_lowest_cost(
            self.ctx,
            sdb,
            self.ctx.target_storage,
            self.ctx.target_storage.natural_storage_format,
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
