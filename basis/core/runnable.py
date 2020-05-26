from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Generator, List, Optional

import sqlalchemy
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import Session

from basis.core.conversion import convert_lowest_cost
from basis.core.data_function import (
    BoundDataFunctionInterface,
    BoundTypedDataAnnotation,
    ConcreteTypedDataAnnotation,
    ConfiguredDataFunction,
    ConfiguredDataFunctionGraph,
    DataFunction,
    DataFunctionInterfaceManager,
    DataFunctionLog,
    DataInterfaceType,
    DataResourceLog,
    Direction,
    InputExhaustedException,
    PythonDataFunction,
)
from basis.core.data_resource import (
    DataResourceMetadata,
    DataSetMetadata,
    LocalMemoryDataRecords,
    ManagedDataResource,
    StoredDataResourceMetadata,
)
from basis.core.environment import Environment
from basis.core.metadata.orm import BaseModel
from basis.core.runtime import RuntimeClass, RuntimeEngine, Runtime
from basis.core.storage import LocalMemoryStorageEngine, Storage
from basis.utils.common import (
    cf,
    error_symbol,
    get_spinner,
    printd,
    success_symbol,
    utcnow,
)

logger = logging.getLogger(__name__)


class Language(Enum):
    PYTHON = "python"
    SQL = "sql"


class LanguageDialect(Enum):
    CPYTHON = (Language.PYTHON, "cpython")
    POSTGRES = (Language.SQL, "postgres")
    MYSQL = (Language.SQL, "mysql")


@dataclass(frozen=True)
class LanguageDialectSupport:
    language_dialect: LanguageDialect
    version_support: str  # e.g. >3.6.3,<4.0


@dataclass(frozen=True)
class CompiledDataFunction:  # TODO: this is unused currently, just a dumb wrapper on the df
    key: str
    # code: str
    function: DataFunction  # TODO: compile this to actual string code we can run aaannnnnnyyywhere
    # language_support: Iterable[LanguageDialect] = None  # TODO


@dataclass(frozen=True)
class RuntimeSpecification:
    runtime_class: RuntimeClass
    runtime_engine: RuntimeEngine
    runtime_version_requirement: str
    package_requirements: List[str]  # "extensions" for postgres, etc


@dataclass(frozen=True)
class Runnable:
    configured_data_function_key: str
    compiled_datafunction: CompiledDataFunction
    # runtime_specification: RuntimeSpecification # TODO: support this
    datafunction_interface: BoundDataFunctionInterface


@dataclass(frozen=True)
class RunSession:
    datafunction_log: DataFunctionLog
    metadata_session: Session  # Make this a URL or other jsonable and then runtime can connect

    def log(self, dr: DataResourceMetadata, direction: Direction):
        drl = DataResourceLog(  # type: ignore
            data_function_log=self.datafunction_log,
            data_resource=dr,
            direction=direction,
            processed_at=utcnow(),
        )
        self.metadata_session.add(drl)

    def log_input(self, dr: DataResourceMetadata):
        printd(f"\t\tLogging Input {dr}")
        self.log(dr, Direction.INPUT)

    def log_output(self, dr: DataResourceMetadata):
        printd(f"\t\tLogging Output {dr}")
        self.log(dr, Direction.OUTPUT)


@dataclass  # (frozen=True)
class ExecutionContext:
    env: Environment
    metadata_session: Session
    storages: List[Storage]
    runtimes: List[Runtime]
    target_storage: Storage
    local_memory_storage: Storage
    current_runtime: Optional[Runtime] = None

    def clone(self, **kwargs):
        args = dict(
            env=self.env,
            metadata_session=self.metadata_session,
            storages=self.storages,
            runtimes=self.runtimes,
            target_storage=self.target_storage,
            local_memory_storage=self.local_memory_storage,
            current_runtime=self.current_runtime,
        )
        args.update(**kwargs)
        return ExecutionContext(**args)  # type: ignore

    @contextmanager
    def start_data_function_run(
        self, cdf_key: str
    ) -> Generator[RunSession, None, None]:
        assert self.current_runtime is not None, "Runtime not set"
        dfl = DataFunctionLog(  # type: ignore
            configured_data_function_key=cdf_key,
            runtime_url=self.current_runtime.url,
            started_at=utcnow(),
        )
        try:
            yield RunSession(dfl, self.metadata_session)
        except Exception as e:
            raise e
        finally:
            dfl.completed_at = utcnow()

    def add(self, obj: BaseModel) -> BaseModel:
        try:
            self.metadata_session.add(obj)
        except InvalidRequestError as e:
            # Already in session perhaps
            printd(f"Can't add obj {obj}: {e}")
        return self.merge(obj)

    def merge(self, obj: BaseModel) -> BaseModel:
        return self.metadata_session.merge(obj)

    @property
    def all_storages(self) -> List[Storage]:
        # TODO: should it be in the list of storages already?
        return [self.local_memory_storage] + self.storages


@dataclass(frozen=True)
class DataFunctionContext:
    execution_context: ExecutionContext
    worker: Worker
    runnable: Runnable
    inputs: List[BoundTypedDataAnnotation]
    output: Optional[ConcreteTypedDataAnnotation]


class ExecutionManager:
    def __init__(self, ctx: ExecutionContext):
        self.ctx = ctx
        self.env = ctx.env

    def select_runtime(self, cdf: ConfiguredDataFunction) -> Runtime:
        from basis.core.sql.data_function import SqlDataFunction

        # TODO: not happy with runtime <-> DF mapping here. Let's rethink
        try:
            cls = cdf.datafunction.runtime_class
        except AttributeError:
            if isinstance(cdf.datafunction, SqlDataFunction):
                cls = RuntimeClass.DATABASE
            elif isinstance(cdf.datafunction, PythonDataFunction):
                cls = RuntimeClass.PYTHON
            else:
                # cls = RuntimeClass.PYTHON
                raise NotImplementedError(cdf.datafunction)  # TODO
        for runtime in self.ctx.runtimes:
            if runtime.runtime_class == cls:  # TODO: Just taking the first one...
                return runtime
        raise Exception(
            f"No compatible runtime available for {cdf} (runtime class {cls} required)"
        )

    def get_bound_data_function_interface(
        self, cdf: ConfiguredDataFunction
    ) -> BoundDataFunctionInterface:
        dfi_mgr = DataFunctionInterfaceManager(self.ctx, cdf)
        return dfi_mgr.get_bound_interface()

    def run_graph(
        self, cdf: ConfiguredDataFunctionGraph, to_exhaustion: bool = False
    ) -> Optional[ManagedDataResource]:
        output: Optional[ManagedDataResource] = None
        for child in cdf.get_cdfs():
            output = self.run(child, to_exhaustion=to_exhaustion)
        return output

    def run(
        self, cdf: ConfiguredDataFunction, to_exhaustion: bool = False
    ) -> Optional[ManagedDataResource]:
        if cdf.is_graph():  # cdf: ConfiguredDataFunctionGraph
            return self.run_graph(cdf, to_exhaustion=to_exhaustion)
        runtime = self.select_runtime(cdf)
        run_ctx = self.ctx.clone(current_runtime=runtime)
        worker = Worker(run_ctx)
        last_output: Optional[DataResourceMetadata] = None

        # Setup for run
        base_msg = (
            f"Running node: {cf.green(cdf.key)} {cf.dimmed(cdf.datafunction.key)}"
        )
        spinner = get_spinner()
        spinner.start(base_msg)
        start = time.time()
        n_outputs = 0
        n_runs = 0
        try:
            while True:
                dfi = self.get_bound_data_function_interface(cdf)
                runnable = Runnable(
                    configured_data_function_key=cdf.key,
                    compiled_datafunction=CompiledDataFunction(
                        key=cdf.key, function=cdf.datafunction
                    ),
                    datafunction_interface=dfi,
                )
                last_output = worker.run(runnable)
                n_runs += 1
                if last_output is not None:
                    n_outputs += 1
                if (
                    not to_exhaustion or not dfi.inputs
                ):  # TODO: We just run no-input DFs (source extractors) once no matter what
                    break
                spinner.text = f"{base_msg}: {cf.blue}{cf.bold(n_outputs)} {cf.dimmed_blue}DataResources output{cf.reset} {cf.dimmed}{(time.time() - start):.1f}s{cf.reset}"
            spinner.stop_and_persist(symbol=cf.success(success_symbol))
        except InputExhaustedException as e:  # TODO: i don't think we need this out here anymore (now that extractors don't throw)
            printd(cf.warning("    Input Exhausted"))
            if e.args:
                printd(e)
            if n_runs == 0:
                spinner.stop_and_persist(
                    symbol=cf.success(success_symbol),
                    text=f"{base_msg}: {cf.dimmed_bold}No unprocessed inputs{cf.reset}",
                )
            else:
                spinner.stop_and_persist(symbol=cf.success(success_symbol))
        except Exception as e:
            spinner.stop_and_persist(
                symbol=cf.error(error_symbol),
                text=f"{base_msg}: {cf.error('Error')} {cf.dimmed_red(e)}",
            )
            raise e

        if last_output is None:
            return None
        new_session = self.env.get_new_metadata_session()
        last_output = new_session.merge(last_output)
        return last_output.as_managed_data_resource(self.ctx,)  # type: ignore  # Doesn't understand merge


class Worker:
    def __init__(self, ctx: ExecutionContext):
        self.env = ctx.env
        self.ctx = ctx

    def run(self, runnable: Runnable) -> Optional[DataResourceMetadata]:
        output_dr: Optional[DataResourceMetadata] = None
        with self.ctx.start_data_function_run(
            runnable.configured_data_function_key
        ) as run_session:
            output = self.execute_datafunction(runnable)
            if output is not None:
                assert runnable.datafunction_interface.output is not None
                assert (
                    self.ctx.target_storage is not None
                ), "Must specify target storage for output"
                output_dr = self.conform_output(run_session, output, runnable)
                assert output_dr is not None, output
                run_session.log_output(output_dr)

            for input in runnable.datafunction_interface.inputs:
                assert input.data_resource is not None, input
                run_session.log_input(input.data_resource)
        return output_dr

    def execute_datafunction(self, runnable: Runnable) -> DataInterfaceType:
        args = []
        if runnable.datafunction_interface.requires_data_function_context:
            dfc = DataFunctionContext(
                self.ctx,
                worker=self,
                runnable=runnable,
                inputs=runnable.datafunction_interface.inputs,
                output=runnable.datafunction_interface.output,
            )
            args.append(dfc)
        inputs = runnable.datafunction_interface.as_kwargs()
        mgd_inputs = {n: self.get_managed_data_resource(dr) for n, dr in inputs.items()}
        return runnable.compiled_datafunction.function(*args, **mgd_inputs)

    def get_managed_data_resource(
        self, dr: DataResourceMetadata
    ) -> ManagedDataResource:
        return dr.as_managed_data_resource(self.ctx)

    def conform_output(
        self, worker_session: RunSession, output: DataInterfaceType, runnable: Runnable,
    ) -> DataResourceMetadata:
        assert runnable.datafunction_interface.output is not None
        assert self.ctx.target_storage is not None
        # TODO: check if these Metadata objects have been added to session!
        #   also figure out what merge actually does
        if isinstance(output, StoredDataResourceMetadata):
            output = self.ctx.merge(output)
            return output.data_resource
        if isinstance(output, DataResourceMetadata):
            output = self.ctx.merge(output)
            return output
        if isinstance(output, DataSetMetadata):
            output = self.ctx.merge(output)
            return output.data_resource

        ldr = LocalMemoryDataRecords.from_records_object(output)
        dr = DataResourceMetadata(
            otype_uri=runnable.datafunction_interface.output.otype.uri
        )
        sdr = StoredDataResourceMetadata(  # type: ignore
            data_resource=dr,
            storage_url=self.ctx.local_memory_storage.url,
            data_format=ldr.data_format,
        )
        dr = self.ctx.add(dr)
        sdr = self.ctx.add(sdr)
        LocalMemoryStorageEngine(
            self.env, self.ctx.local_memory_storage
        ).store_local_memory_data_records(sdr, ldr)
        # Place output in target storage
        convert_lowest_cost(
            self.ctx,
            sdr,
            self.ctx.target_storage,
            self.ctx.target_storage.natural_storage_format,
        )
        return dr

    # TODO: where does this sql stuff really belong?
    def get_connection(self) -> sqlalchemy.engine.Engine:
        if self.ctx.current_runtime is None:
            raise Exception("Current runtime not set")
        if self.ctx.current_runtime.runtime_class != RuntimeClass.DATABASE:
            raise Exception(f"Runtime not supported {self.ctx.current_runtime}")
        return sqlalchemy.create_engine(self.ctx.current_runtime.url)

    def execute_sql(self, sql: str) -> ResultProxy:
        printd("Executing SQL:")
        printd(sql)
        return self.get_connection().execute(sql)
