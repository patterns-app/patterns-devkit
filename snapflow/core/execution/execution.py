from __future__ import annotations

from collections import abc, defaultdict
from contextlib import contextmanager
from sys import executable
from typing import TYPE_CHECKING, Any, Callable, Iterable, List

import dcp
import sqlalchemy
from dcp.utils.common import rand_str, utcnow
from loguru import logger
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.execution import (
    DebugMetadataExecutionResultHandler,
    ExecutableCfg,
    ExecutionResult,
    MetadataExecutionResultHandler,
    PythonException,
    RemoteCallbackMetadataExecutionResultHandler,
    get_global_metadata_result_handler,
)
from snapflow.core.declarative.function import DEFAULT_OUTPUT_NAME
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment
from snapflow.core.execution.context import DataFunctionContext
from snapflow.core.function import (
    DataFunction,
    DataInterfaceType,
    InputExhaustedException,
)
from snapflow.utils.output import cf, error_symbol, success_symbol
from sqlalchemy.sql.expression import select


def run_dataspace(ds: DataspaceCfg):
    env = Environment(ds)
    env.run_graph(ds.graph)


class ImproperlyStoredDataBlockException(Exception):
    pass


# def validate_data_blocks(env: Environment):
#     # TODO: More checks?
#     env.md_api.flush()
#     for obj in env.md_api.active_session.identity_map.values():
#         if isinstance(obj, DataBlockMetadata):
#             urls = set([sdb.storage_url for sdb in obj.stored_data_blocks])
#             if all(u.startswith("python") for u in urls):
#                 fmts = set([sdb.data_format for sdb in obj.stored_data_blocks])
#                 if all(not f.is_storable() for f in fmts):
#                     raise ImproperlyStoredDataBlockException(
#                         f"DataBlock {obj} is not properly stored (no storeable format(s): {fmts})"
#                     )


class ExecutionLogger:
    def __init__(self, out: Callable = lambda x: print(x, end="")):
        self.out = out
        self.curr_indent = 0
        self.indent_size = 4

    @contextmanager
    def indent(self, n=1):
        self.curr_indent += n * self.indent_size
        yield
        self.curr_indent = max(self.curr_indent - n * self.indent_size, 0)

    def log(self, msg: str, prefix="", suffix="", indent: int = 0):
        total_indent = self.curr_indent + indent * self.indent_size
        lines = msg.strip("\n").split("\n")
        full_prefix = total_indent * " " + prefix
        sep = suffix + "\n" + full_prefix
        message = full_prefix + sep.join(lines) + suffix
        self.out(message)
        if msg.endswith("\n"):
            self.out("\n")

    def log_token(self, msg: str):
        self.out(msg)


class ExecutionManager:
    def __init__(self, exe: ExecutableCfg):
        self.exe = exe
        self.logger = ExecutionLogger()
        self.node = self.exe.node
        self.function = exe.get_library().get_function(self.node.function)
        self.start_time = utcnow()
        self.cfg = exe.execution_config
        self.to_exhaustion = True

    def execute(self) -> List[ExecutionResult]:
        # Setup for run
        base_msg = (
            f"Running node {cf.bold(self.node.key)} {cf.dimmed(self.function.key)}\n"
        )
        logger.debug(
            f"RUNNING NODE {self.node.key} {self.function.key} with params `{self.node.params}`"
        )
        self.exe.function_log.started_at = utcnow()
        logger.debug(self.exe)
        self.logger.log(base_msg)
        results = []
        # try:
        for inputs in self.exe.bound_interface.iter_as_function_kwarg_inputs():
            result = self._execute_inputs(inputs)
            self.publish_result(result)
            results.append(result)
            if result.has_error():
                break
        # except Exception as e:
        #     self.exe.function_log.error = PythonException.from_exception(e).dict()
        #     raise e
        # finally:
        #     self.exe.function_log.completed_at = utcnow()
        # self.publish_result()
        return results

    def _execute_inputs(self, inputs) -> ExecutionResult:
        with self.logger.indent():
            # self.logger.log(f"Running inputs {inputs}")
            ctx = self.prepare_context(inputs)
            try:
                self._call_data_function(ctx)
            except Exception as e:
                ctx.result.function_error = PythonException.from_exception(e)
            result = ctx.result
            if not result.has_error():
                ctx.log_inputs()
            self.log_execution_result(result)
            if not result.has_error():
                self.logger.log(cf.success("Ok " + success_symbol + "\n"))  # type: ignore
            else:
                error = result.function_error or result.framework_error
                error_msg = error.error or "DataFunction failed (unknown error)"
                self.logger.log(cf.error("Error " + error_symbol + " " + cf.dimmed(error_msg[:80])) + "\n")  # type: ignore
                if result.function_error.traceback:
                    self.logger.log(cf.dimmed(result.function_error.traceback), indent=2)  # type: ignore
            logger.debug(f"Execution result: {result}")
            logger.debug(f"*DONE* RUNNING NODE {self.node.key} {self.function.key}")
        return result

    def publish_result(self, result: ExecutionResult):
        # TODO: support alternate reporters
        result = result.finalize()  # TODO: pretty important step!
        handler = self.exe.execution_config.result_handler
        if handler.type == MetadataExecutionResultHandler.__name__:
            get_global_metadata_result_handler()(self.exe, result)
        elif handler.type == DebugMetadataExecutionResultHandler.__name__:
            DebugMetadataExecutionResultHandler()(self.exe, result)
        elif handler.type == RemoteCallbackMetadataExecutionResultHandler.__name__:
            RemoteCallbackMetadataExecutionResultHandler(**handler.cfg)(
                self.exe, result
            )
        else:
            raise NotImplementedError(handler.type)

    def prepare_context(self, inputs) -> DataFunctionContext:
        return DataFunctionContext(
            dataspace=self.cfg.dataspace,
            function=self.function,
            node=self.node,
            executable=self.exe,
            result=ExecutionResult(),
            inputs=inputs,
            execution_start_time=utcnow(),
            library=self.exe.get_library(),
        )

    def _call_data_function(self, ctx: DataFunctionContext):
        function_args, function_kwargs = ctx.get_function_args()
        output_obj = self.function.function_callable(
            *function_args,
            **function_kwargs,
        )
        if output_obj is not None:
            self.emit_output_object(ctx, output_obj)
            # TODO: update node state block counts?

    def emit_output_object(
        self, ctx: DataFunctionContext, output_obj: DataInterfaceType
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
            ctx.emit(output_obj)

    def log_execution_result(self, result: ExecutionResult):
        self.logger.log("Inputs: ")
        if result.input_blocks_consumed:
            self.logger.log("\n")
            with self.logger.indent():
                for input_name, blocks in result.input_blocks_consumed.items():
                    if len(blocks) > 1:
                        self.logger.log(
                            f"{input_name}: {len(blocks)} blocks processed\n"
                        )
                    elif len(blocks) == 1:
                        self.logger.log(f"{input_name}: {blocks[0].id}\n")
                    else:
                        self.logger.log(f"{input_name}: No blocks processed\n")
        else:
            # if not result.non_reference_inputs_bound:
            #     self.logger.log_token("n/a\n")
            # else:
            self.logger.log_token("None\n")
        self.logger.log("Outputs: ")
        if result.output_blocks_emitted:
            self.logger.log("\n")
            with self.logger.indent():
                for output_name, block in result.output_blocks_emitted.items():
                    self.logger.log(f"{output_name}: ")
                    cnt = block.record_count
                    # alias = block.alias
                    if cnt is not None:
                        self.logger.log_token(f" {cnt} records ")
                    self.logger.log_token(cf.dimmed(f"{block.id}\n"))  # type: ignore
        else:
            self.logger.log_token("None\n")
