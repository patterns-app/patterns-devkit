from __future__ import annotations

import json
import time
from collections import abc
from contextlib import contextmanager
from dataclasses import MISSING, dataclass, field
from enum import Enum
from itertools import tee
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Generic,
    List,
    Optional,
    Union,
    cast,
)

import sqlalchemy
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import Session

from dags.core.conversion import convert_lowest_cost
from dags.core.data_block import (
    DataBlock,
    DataBlockMetadata,
    DataSetMetadata,
    LocalMemoryDataRecords,
    ManagedDataBlock,
    StoredDataBlockMetadata,
    create_data_block_from_records,
)
from dags.core.data_formats import DataFrameGenerator, RecordsListGenerator
from dags.core.data_formats.base import ReusableGenerator
from dags.core.environment import Environment
from dags.core.metadata.orm import BaseModel
from dags.core.node import DataBlockLog, Direction, Node, PipeLog, get_state
from dags.core.pipe import (
    DataInterfaceType,
    InputExhaustedException,
    Pipe,
    PipeInterface,
)
from dags.core.pipe_interface import (
    BoundPipeInterface,
    NodeInput,
    NodeInterfaceManager,
    PipeAnnotation,
)
from dags.core.runtime import Runtime, RuntimeClass, RuntimeEngine
from dags.core.storage.storage import LocalMemoryStorageEngine, Storage
from dags.core.typing.object_schema import ObjectSchema
from dags.utils.common import (
    DagsJSONEncoder,
    cf,
    error_symbol,
    printd,
    success_symbol,
    utcnow,
)
from dags.utils.typing import C, S
from loguru import logger

if TYPE_CHECKING:
    from dags.core.graph import Graph


class Language(Enum):
    PYTHON = "python"
    SQL = "sql"


class LanguageDialect(Enum):
    CPYTHON = (Language.PYTHON, "cpython")
    POSTGRES = (Language.SQL, "postgres")
    MYSQL = (Language.SQL, "mysql")


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
    pipe_interface: BoundPipeInterface
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
    logger: Optional[Callable[[str], None]] = None

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
            logger=logger,
        )
        args.update(**kwargs)
        return ExecutionContext(**args)  # type: ignore

    @contextmanager
    def start_pipe_run(self, node: Node) -> Generator[RunSession, None, None]:
        assert self.current_runtime is not None, "Runtime not set"
        node_state = node.get_state(self.metadata_session) or {}
        dfl = PipeLog(  # type: ignore
            node_key=node.key,
            node_start_state=node_state,
            node_end_state=node_state,
            pipe_key=node.pipe.key,
            pipe_config=node.config,
            runtime_url=self.current_runtime.url,
            started_at=utcnow(),
        )
        try:
            yield RunSession(dfl, self.metadata_session)
            # Only persist state on successful run
            dfl.persist_state(self.metadata_session)
        except Exception as e:
            dfl.set_error(e)
            raise e
        finally:
            dfl.completed_at = utcnow()

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
    inputs: List[NodeInput]
    pipe_log: PipeLog
    # state: Dict = field(default_factory=dict)
    # emitted_states: List[Dict] = field(default_factory=list)
    # resolved_output_schema: Optional[ObjectSchema]
    # realized_output_schema: Optional[ObjectSchema]

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


class ExecutionManager:
    def __init__(self, ctx: ExecutionContext):
        self.ctx = ctx
        self.env = ctx.env

    def get_logger(self) -> Callable[[str], None]:
        return self.ctx.logger or (lambda s: print(s, end=""))

    def log(self, msg: str):
        self.get_logger()(msg)

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

    def get_bound_pipe_interface(self, node: Node) -> BoundPipeInterface:
        dfi_mgr = NodeInterfaceManager(self.ctx, node)
        return dfi_mgr.get_bound_interface()

    def run(self, node: Node, to_exhaustion: bool = False) -> Optional[DataBlock]:
        if node.is_composite():
            raise NotImplementedError
        runtime = self.select_runtime(node)
        run_ctx = self.ctx.clone(current_runtime=runtime)
        worker = Worker(run_ctx)
        last_output: Optional[DataBlockMetadata] = None

        # Setup for run
        base_msg = f"Running node {cf.bold(node.key)} {cf.dimmed(node.pipe.key)}"
        if node.declared_composite_node_key is not None:
            base_msg = " " * 8 + f" {cf.dimmed(node.pipe.key)}"
        self.log("{0:50} ".format(str(base_msg)))
        # self.log(base_msg)
        # start = time.time()
        n_outputs = 0
        n_runs = 0
        try:
            while True:
                dfi = self.get_bound_pipe_interface(node)
                df = node.pipe
                if df is None:
                    raise NotImplementedError(
                        f"No pipe definition found for {node.pipe.key} and runtime {runtime.runtime_class}"
                    )
                runnable = Runnable(
                    node_key=node.key,
                    compiled_pipe=CompiledPipe(key=node.key, pipe=df,),
                    pipe_interface=dfi,
                    configuration=node.config,
                )
                last_output = worker.run(runnable)
                n_runs += 1
                if last_output is not None:
                    n_outputs += 1
                if (
                    not to_exhaustion or not dfi.inputs
                ):  # TODO: We just run no-input DFs (source extractors) once no matter what
                    # (they are responsible for creating their own generators)
                    break
                # This below could work, but some sources may always return a
                # result (eg datetime strictly equal edge case), and this would
                # be infinite loop in that case
                # if not dfi.inputs:
                #     # No inputs, it is a data source (extractor), we stop when it produces no more output?
                #     if last_output is None:
                #         break
                # spinner.text = f"{base_msg}: {cf.blue}{cf.bold(n_outputs)} {cf.dimmed_blue}DataBlocks output{cf.reset} {cf.dimmed}{(time.time() - start):.1f}s{cf.reset}"
            self.log(cf.success(success_symbol + "\n"))
            # spinner.stop_and_persist(symbol=cf.success(success_symbol))
        except InputExhaustedException as e:  # TODO: i don't think we need this out here anymore (now that extractors don't throw)
            logger.debug(cf.warning("    Input Exhausted"))
            if e.args:
                logger.debug(e)
            if n_runs == 0:
                self.log(cf.success(success_symbol) + " No unprocessed inputs\n")
            else:
                self.log(cf.success(success_symbol + "\n"))
        except Exception as e:
            self.log(cf.error(error_symbol + " Error \n") + str(e) + "\n")
            raise e

        if last_output is None:
            return None
        new_session = self.env.get_new_metadata_session()
        last_output = new_session.merge(last_output)
        return last_output.as_managed_data_block(self.ctx)  # type: ignore # (mypy does not know merge() is safe)

    #
    # def produce(
    #         self, node_like: Union[ConfiguredPipe, str]
    # ) -> Optional[DataBlock]:
    #     if isinstance(node_like, str):
    #         node_like = self.env.get_node(node_like)
    #     assert isinstance(node_like, ConfiguredPipe)
    #     dependencies = get_all_upstream_dependencies_in_execution_order(self.env, node_like)
    #     for dep in dependencies:
    #         with self.env.execution() as em:
    #             em.run(dep, to_exhaustion=True)


class Worker:
    def __init__(self, ctx: ExecutionContext):
        self.env = ctx.env
        self.ctx = ctx

    def run(self, runnable: Runnable) -> Optional[DataBlockMetadata]:
        output_block: Optional[DataBlockMetadata] = None
        node = self.ctx.graph.get_any_node(runnable.node_key)
        with self.ctx.start_pipe_run(node) as run_session:
            output = self.execute_pipe(runnable, run_session)
            if output is not None:
                # assert (
                #     runnable.pipe_interface.resolved_output_schema is not None
                # )
                assert (
                    self.ctx.target_storage is not None
                ), "Must specify target storage for output"
                output_block = self.conform_output(run_session, output, runnable)
                # assert output_block is not None, output    # Output block may be none if empty generator
                if output_block is not None:
                    run_session.log_output(output_block)

            for input in runnable.pipe_interface.inputs:
                # assert input.bound_data_block is not None, input
                if input.bound_data_block is not None:
                    run_session.log_input(input.bound_data_block)
        return output_block

    def execute_pipe(
        self, runnable: Runnable, run_session: RunSession
    ) -> DataInterfaceType:
        args = []
        if runnable.pipe_interface.requires_pipe_context:
            dfc = PipeContext(
                self.ctx,
                worker=self,
                runnable=runnable,
                inputs=runnable.pipe_interface.inputs,
                pipe_log=run_session.pipe_log,
                # resolved_output_schema=runnable.pipe_interface.resolved_output_schema,
                # realized_output_schema=runnable.pipe_interface.realized_output_schema,
            )
            args.append(dfc)
        inputs = runnable.pipe_interface.as_kwargs()
        mgd_inputs = {
            n: self.get_managed_data_block(block) for n, block in inputs.items()
        }
        assert (
            runnable.compiled_pipe.pipe.pipe_callable is not None
        ), f"Attempting to execute composite pipe directly {runnable.compiled_pipe.pipe}"
        return runnable.compiled_pipe.pipe.pipe_callable(*args, **mgd_inputs)

    def get_managed_data_block(self, block: DataBlockMetadata) -> ManagedDataBlock:
        return block.as_managed_data_block(self.ctx)

    def conform_output(
        self, worker_session: RunSession, output: DataInterfaceType, runnable: Runnable,
    ) -> Optional[DataBlockMetadata]:
        assert runnable.pipe_interface.output is not None
        # assert runnable.pipe_interface.resolved_output_schema is not None
        assert self.ctx.target_storage is not None
        # TODO: check if these Metadata objects have been added to session!
        #   also figure out what merge actually does
        if isinstance(output, StoredDataBlockMetadata):
            output = self.ctx.merge(output)
            return output.data_block
        if isinstance(output, DataBlockMetadata):
            output = self.ctx.merge(output)
            return output
        if isinstance(output, DataSetMetadata):
            output = self.ctx.merge(output)
            return output.data_block

        if isinstance(output, abc.Generator):
            if runnable.pipe_interface.output.data_format_class == "DataFrameGenerator":
                output = DataFrameGenerator(output)
            elif (
                runnable.pipe_interface.output.data_format_class
                == "RecordsListGenerator"
            ):
                output = RecordsListGenerator(output)
            else:
                TypeError(output)
            output = cast(ReusableGenerator, output)
            if output.get_one() is None:
                # Empty generator
                return None
        block, sdb = create_data_block_from_records(
            self.env,
            self.ctx.metadata_session,
            self.ctx.local_memory_storage,
            output,
            # expected_schema=runnable.pipe_interface.resolved_output_schema,
        )
        # ldr = LocalMemoryDataRecords.from_records_object(output)
        # block = DataBlockMetadata(
        #     schema_key=runnable.pipe_interface.output_schema.key
        # )
        # sdb = StoredDataBlockMetadata(  # type: ignore
        #     data_block=block,
        #     storage_url=self.ctx.local_memory_storage.url,
        #     data_format=ldr.data_format,
        # )
        # block = self.ctx.add(block)
        # sdb = self.ctx.add(sdb)
        # LocalMemoryStorageEngine(
        #     self.env, self.ctx.local_memory_storage
        # ).store_local_memory_data_records(sdb, ldr)

        # TODO: check if existing storage_format is compatible with target storage, instead of using natural (no need to convert then)
        # Place output in target storage
        convert_lowest_cost(
            self.ctx,
            sdb,
            self.ctx.target_storage,
            self.ctx.target_storage.natural_storage_format,
        )
        return block

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
