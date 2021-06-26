from __future__ import annotations

from typing import List, Optional, Set, Tuple

from dcp.utils.common import rand_str, utcnow
from loguru import logger
from snapflow.core.data_block import DataBlock
from snapflow.core.declarative.dataspace import DataspaceCfg
from snapflow.core.declarative.execution import (
    ExecutableCfg,
    ExecutionCfg,
    ExecutionResult,
    MetadataExecutionResultHandler,
    set_global_metadata_result_handler,
)
from snapflow.core.declarative.function import DEFAULT_OUTPUT_NAME
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.environment import Environment
from snapflow.core.execution.execution import ExecutionManager
from snapflow.core.function import (
    DataFunction,
    DataInterfaceType,
    InputExhaustedException,
)
from snapflow.core.function_interface_manager import get_bound_interface
from snapflow.core.persistence.data_block import (
    Alias,
    DataBlockMetadata,
    StoredDataBlockMetadata,
    get_datablock_id,
    get_stored_datablock_id,
)
from snapflow.core.persistence.schema import GeneratedSchema
from snapflow.core.persistence.state import (
    DataBlockLog,
    DataFunctionLog,
    Direction,
    NodeState,
    get_or_create_state,
    get_state,
)
from snapflow.utils.output import cf, error_symbol, success_symbol
from sqlalchemy.sql.expression import select


def run_dataspace(ds: DataspaceCfg):
    env = Environment(ds)
    env.run_graph(ds.graph)


class ImproperlyStoredDataBlockException(Exception):
    pass


# # TODO: use this
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


def prepare_executable(
    env: Environment, cfg: ExecutionCfg, node: GraphCfg, graph: GraphCfg
) -> ExecutableCfg:
    global global_metadata_result_handler

    with env.md_api.begin():
        md = env.md_api
        try:
            bound_interface = get_bound_interface(env, cfg, node, graph)
        except InputExhaustedException as e:
            logger.debug(f"Inputs exhausted {e}")
            raise e
            # return ExecutionResult.empty()
        node_state_obj = get_or_create_state(env, node.key)
        if node_state_obj is None:
            node_state = {}
        else:
            node_state = node_state_obj.state or {}

        function_log = DataFunctionLog(  # type: ignore
            node_key=node.key,
            node_start_state=node_state.copy(),  # {k: v for k, v in node_state.items()},
            node_end_state=node_state,
            function_key=node.function,
            function_params=node.params,
            # runtime_url=self.current_runtime.url,
            queued_at=utcnow(),
        )
        node_state_obj.latest_log = function_log
        md.add(function_log)
        md.add(node_state_obj)
        md.flush([function_log, node_state_obj])

        # TODO: runtime and result handler

        exe = ExecutableCfg(
            node_key=node.key,
            graph=graph,
            execution_config=cfg,
            bound_interface=bound_interface,
            function_log=function_log,
        )
        set_global_metadata_result_handler(MetadataExecutionResultHandler(env))
        return exe

        # Validate local memory objects: Did we leave any non-storeables hanging?
        # validate_data_blocks(self.env)

    # # TODO: goes somewhere else
    #     except Exception as e:
    #         # Don't worry about exhaustion exceptions
    #         if not isinstance(e, InputExhaustedException):
    #             logger.debug(f"Error running node:\n{traceback.format_exc()}")
    #             function_log.set_error(e)
    #             function_log.persist_state(self.env)
    #             function_log.completed_at = utcnow()
    #             # TODO: should clean this up so transaction surrounds things that you DO
    #             #       want to rollback, obviously
    #             # md.commit()  # MUST commit here since the re-raised exception will issue a rollback
    #             if (
    #                 self.exe.execution_config.dataspace.snapflow.abort_on_function_error
    #             ):  # TODO: from call or env
    #                 raise e
    #     finally:
    #         function_ctx.finish_execution()
    #         # Persist state on success OR error:
    #         function_log.persist_state(self.env)
    #         function_log.completed_at = utcnow()

    #     with self.start_function_run(self.node, bound_interface) as function_ctx:
    #         # function = executable.compiled_function.function
    #         # local_vars = locals()
    #         # if hasattr(function, "_locals"):
    #         #     local_vars.update(function._locals)
    #         # exec(function.get_source_code(), globals(), local_vars)
    #         # output_obj = local_vars[function.function_callable.__name__](
    #         function_args, function_kwargs = function_ctx.get_function_args()
    #         output_obj = function_ctx.function.function_callable(
    #             *function_args, **function_kwargs,
    #         )
    #         if output_obj is not None:
    #             self.emit_output_object(output_obj, function_ctx)
    #     result = function_ctx.as_execution_result()
    #     # TODO: update node state block counts?
    # logger.debug(f"EXECUTION RESULT {result}")
    # return result

    # def emit_output_object(
    #     self, output_obj: DataInterfaceType, function_ctx: DataFunctionContext,
    # ):
    #     assert output_obj is not None
    #     if isinstance(output_obj, abc.Generator):
    #         output_iterator = output_obj
    #     else:
    #         output_iterator = [output_obj]
    #     i = 0
    #     for output_obj in output_iterator:
    #         logger.debug(output_obj)
    #         i += 1
    #         function_ctx.emit(output_obj)

    # def log_execution_result(self, result: ExecutionResult):
    #     self.logger.log("Inputs: ")
    #     if result.input_block_counts:
    #         self.logger.log("\n")
    #         with self.logger.indent():
    #             for input_name, cnt in result.input_block_counts.items():
    #                 self.logger.log(f"{input_name}: {cnt} block(s) processed\n")
    #     else:
    #         if not result.non_reference_inputs_bound:
    #             self.logger.log_token("n/a\n")
    #         else:
    #             self.logger.log_token("None\n")
    #     self.logger.log("Outputs: ")
    #     if result.output_blocks:
    #         self.logger.log("\n")
    #         with self.logger.indent():
    #             for output_name, block_summary in result.output_blocks.items():
    #                 self.logger.log(f"{output_name}:")
    #                 cnt = block_summary["record_count"]
    #                 alias = block_summary["alias"]
    #                 if cnt is not None:
    #                     self.logger.log_token(f" {cnt} records")
    #                 self.logger.log_token(
    #                     f" {alias} " + cf.dimmed(f"({block_summary['id']})\n")  # type: ignore
    #                 )
    #     else:
    #         self.logger.log_token("None\n")


def run(exe: ExecutableCfg, to_exhaustion: bool = True) -> List[ExecutionResult]:
    # TODO: support other runtimes
    results = []
    try:
        results = ExecutionManager(exe).execute()
    except InputExhaustedException:
        pass
    return results
    # while True:
    #     try:
    #         result = em.execute()
    #     except InputExhaustedException:
    #         return cum_result
    #     cum_result.add_result(result)
    #     if (
    #         not to_exhaustion or not result.non_reference_inputs_bound
    #     ):  # TODO: We just run no-input DFs (sources) once no matter what
    #         # (they are responsible for creating their own generators)
    #         break
    #     if cum_result.error:
    #         break
    # return cum_result


def get_latest_output(env: Environment, node: GraphCfg) -> Optional[DataBlock]:
    from snapflow.core.persistence.data_block import DataBlockMetadata

    with env.metadata_api.begin():
        block: DataBlockMetadata = (
            env.md_api.execute(
                select(DataBlockMetadata)
                .join(DataBlockLog)
                .join(DataFunctionLog)
                .filter(
                    DataBlockLog.direction == Direction.OUTPUT,
                    DataFunctionLog.node_key == node.key,
                )
                .order_by(DataBlockLog.created_at.desc())
            )
            .scalars()
            .first()
        )
        if block is None:
            return None
    return block.as_managed_data_block(env)


def ensure_log(
    env: Environment,
    dfl: DataFunctionLog,
    block: DataBlockMetadata,
    direction: Direction,
    name: str,
):
    if env.md_api.execute(
        select(DataBlockLog).filter_by(
            function_log_id=dfl.id,
            stream_name=name,
            data_block_id=block.id,
            direction=direction,
        )
    ).scalar_one_or_none():
        return
    drl = DataBlockLog(  # type: ignore
        function_log_id=dfl.id,
        stream_name=name,
        data_block_id=block.id,
        direction=direction,
        processed_at=block.created_at,
    )
    env.md_api.add(drl)


def save_result(
    env: Environment,
    exe: ExecutableCfg,
    result: ExecutionResult,
):
    # TODO: this should be inside one roll-backable transaction
    save_function_log(env, exe, result)
    dbms: List[DataBlockMetadata] = []
    for input_name, blocks in result.input_blocks_consumed.items():
        for block in blocks:
            ensure_log(env, exe.function_log, block, Direction.INPUT, input_name)
            logger.debug(f"Input logged: {block}")
    for output_name, block in result.output_blocks_emitted.items():
        # if not block.data_is_written:
        #     # TODO: this means we didn't actually ever write an output
        #     # object (usually because we hit an error during output handling)
        #     # TODO: did we really process the inputs then? this is more of a framework-level error
        #     continue
        dbm = DataBlockMetadata(**block.dict())
        dbms.append(dbm)
        env.md_api.add(dbm)
        logger.debug(f"Output logged: {block}")
        ensure_log(env, exe.function_log, block, Direction.OUTPUT, output_name)
    env.md_api.add_all(
        [
            StoredDataBlockMetadata(**s.dict())
            for v in result.stored_blocks_created.values()
            for s in v
        ]
    )
    for s in result.schemas_generated or []:
        env.add_new_generated_schema(s)
    save_state(env, exe, result)
    ensure_aliases(env, exe, result)


def save_function_log(
    env: Environment, exe: ExecutableCfg, result: ExecutionResult
) -> DataFunctionLog:
    if result.function_error:
        exe.function_log.error = result.function_error.dict()
    if (
        exe.function_log.function_params
        and "dataframe" in exe.function_log.function_params
    ):
        # TODO / FIXME: special case hack (don't support dataframe parameters in general!)
        del exe.function_log.function_params["dataframe"]
    if exe.function_log.completed_at is None:
        # TODO: completed at should happen inside the execution? what about multiple inputs?
        exe.function_log.completed_at = utcnow()
    if result.timed_out:
        exe.function_log.timed_out = True
    function_log = env.md_api.merge(DataFunctionLog.from_pydantic(exe.function_log))

    env.md_api.add(function_log)
    return function_log


def save_state(env: Environment, exe: ExecutableCfg, result: ExecutionResult):
    state = env.md_api.execute(
        select(NodeState).filter(NodeState.node_key == exe.node_key)
    ).scalar_one_or_none()
    if state is None:
        state = NodeState(node_key=exe.node_key)
    state.state = exe.function_log.node_end_state
    state.latest_log_id = exe.function_log.id
    state.alias = (
        exe.node.get_alias()
    )  # TODO: not an actual attribute, this gets dropped
    env.md_api.add(state)


def ensure_aliases(env: Environment, exe: ExecutableCfg, result: ExecutionResult):
    # TODO: do for all output streams
    db = result.stdout_block_emitted()
    if db is None:
        return
    if db.id not in result.stored_blocks_created:
        logger.warning(
            f"Missing stored blocks for block: {db.id}, {result.stored_blocks_created}"
        )
        return
    for sdbc in result.stored_blocks_created[db.id]:
        sdb = StoredDataBlockMetadata.from_pydantic(sdbc)
        sdb.create_alias(env, exe.node.get_alias())
