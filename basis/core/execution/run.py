from __future__ import annotations

from typing import List, Optional, Set, Tuple

from basis.core.block import Block
from basis.core.declarative.execution import (
    ExecutableCfg,
    ExecutionCfg,
    ExecutionResult,
)
from basis.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    FunctionSourceFileCfg,
)
from basis.core.environment import Environment
from basis.core.execution.execution import ExecutionManager
from basis.core.function import Function, DataInterfaceType, InputExhaustedException
from basis.core.function_interface_manager import get_bound_interface
from basis.core.persistence.block import (
    Alias,
    BlockMetadata,
    StoredBlockMetadata,
    get_block_id,
)
from basis.core.persistence.schema import GeneratedSchema
from basis.core.persistence.state import (
    Direction,
    get_or_create_state,
    get_state,
)
from basis.utils.output import cf, error_symbol, success_symbol
from dcp.utils.common import rand_str, utcnow
from loguru import logger
from sqlalchemy.sql.expression import select


# def run_dataspace(ds: DataspaceCfg):
#     env = Environment(ds)
#     env.run_graph(ds.graph)


class ImproperlyStoredBlockException(Exception):
    pass


# # TODO: use this
# def validate_blocks(env: Environment):
#     # TODO: More checks?
#     env.md_api.flush()
#     for obj in env.md_api.active_session.identity_map.values():
#         if isinstance(obj, BlockMetadata):
#             urls = set([sdb.storage_url for sdb in obj.stored_blocks])
#             if all(u.startswith("python") for u in urls):
#                 fmts = set([sdb.data_format for sdb in obj.stored_blocks])
#                 if all(not f.is_storable() for f in fmts):
#                     raise ImproperlyStoredBlockException(
#                         f"Block {obj} is not properly stored (no storeable format(s): {fmts})"
#                     )


def prepare_executables_from_result(env: Environment, res: ExecutionResult):
    # get downstream nodes
    nodes: List[NodeCfg] = api.get_downstream_nodes()
    #
    for n in nodes:
        exe = api.get_executable(n, res)
    api.queue_executable(exe)


def prepare_executable(
    env: Environment,
    cfg: ExecutionCfg,
    node: NodeCfg,
    graph: GraphCfg,
    source_file_functions: List[FunctionSourceFileCfg] = [],
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

        function_log = FunctionLog(  # type: ignore
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

        lib_cfg = env.build_library_cfg(graph=graph, interface=bound_interface)
        exe = ExecutableCfg(
            node_key=node.key,
            graph=graph,
            execution_config=cfg,
            bound_interface=bound_interface,
            function_log=function_log,
            library_cfg=lib_cfg,
            source_file_functions=source_file_functions,
        )
        set_global_metadata_result_handler(MetadataExecutionResultHandler(env))
        return exe

        # Validate local memory objects: Did we leave any non-storeables hanging?
        # validate_blocks(self.env)

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
    #                 self.exe.execution_config.dataspace.basis.abort_on_function_error
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
    #     self, output_obj: DataInterfaceType, function_ctx: Context,
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
        # This never happens here? It happens at bind_inputs()
        # TODO: reduce to warn once we track this better
        logger.error(f"Exhausted inputs for {exe.node_key}")
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


def get_latest_output(env: Environment, node: GraphCfg) -> Optional[Block]:
    from basis.core.persistence.block import BlockMetadata

    with env.metadata_api.begin():
        block: BlockMetadata = (
            env.md_api.execute(
                select(BlockMetadata)
                .join(BlockLog)
                .join(FunctionLog)
                .filter(
                    BlockLog.direction == Direction.OUTPUT,
                    FunctionLog.node_key == node.key,
                )
                .order_by(BlockLog.created_at.desc())
            )
            .scalars()
            .first()
        )
        if block is None:
            return None
    return block.as_managed_block(env)


def ensure_log(
    env: Environment,
    dfl: FunctionLog,
    block: BlockMetadata,
    direction: Direction,
    name: str,
):
    if env.md_api.execute(
        select(BlockLog).filter_by(
            function_log_id=dfl.id,
            stream_name=name,
            block_id=block.id,
            direction=direction,
        )
    ).scalar_one_or_none():
        return
    drl = BlockLog(  # type: ignore
        function_log_id=dfl.id,
        stream_name=name,
        block_id=block.id,
        direction=direction,
        processed_at=block.created_at,
    )
    env.md_api.add(drl)


def save_result(
    env: Environment, exe: ExecutableCfg, result: ExecutionResult,
):
    # TODO: this should be inside one roll-backable transaction
    save_function_log(env, exe, result)
    # dbms: List[BlockMetadata] = []
    for input_name, blocks in result.input_blocks_consumed.items():
        for block in blocks:
            ensure_log(env, exe.function_log, block, Direction.INPUT, input_name)
            logger.debug(f"Input logged: {block}")
    for output_name, blocks in result.output_blocks_emitted.items():
        # if not block.data_is_written:
        #     # TODO: this means we didn't actually ever write an output
        #     # object (usually because we hit an error during output handling)
        #     # TODO: did we really process the inputs then? this is more of a framework-level error
        #     continue
        for block in blocks:
            dbm = BlockMetadata(**block.dict())
            # dbms.append(dbm)
            env.md_api.add(dbm)
            logger.debug(f"Output logged: {block}")
            ensure_log(env, exe.function_log, block, Direction.OUTPUT, output_name)
    env.md_api.add_all(
        [
            StoredBlockMetadata(**s.dict())
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
) -> FunctionLog:
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
    function_log = env.md_api.merge(FunctionLog.from_pydantic(exe.function_log))

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
    db = result.latest_stdout_block_emitted()
    if db is None:
        return
    if db.id not in result.stored_blocks_created:
        logger.warning(
            f"Missing stored blocks for block: {db.id}, {result.stored_blocks_created}"
        )
        return
    for sdbc in result.stored_blocks_created[db.id]:
        sdb = StoredBlockMetadata.from_pydantic(sdbc)
        sdb.create_alias(env, exe.node.get_alias())


def make_executable_for_remaining_unprocessed(
    env: Environment, exe: ExecutableCfg, result: ExecutionResult
) -> ExecutableCfg:
    pass


def retry_exe(exe: ExecutableCfg):
    if exe.retries_remaining <= 0:
        raise
    exe = update(exe, retries_remaining=exe.retries_remaining - 1)


def handle_execution_result(
    env: Environment, exe: ExecutableCfg, result: ExecutionResult
):
    handle_execution_result_output_blocks(env, exe, result.output_blocks_emitted)
    remainder_exe = make_executable_for_remaining_unprocessed(env, exe, result)
    # handle retry and requeue based on result
    if result.function_error is not None:
        retry_exe(remainder_exe)
    elif result.timed_out:
        if result.total_blocks_processed() > 0:
            # Timed out with progress, so we can safely requeue remainder
            run_exe(remainder_exe)
        else:
            # Timed out w/o progress, must count as a failure
            retry_exe(remainder_exe)
    elif remainder_exe.has_unprocessed_blocks():
        # unclear why all block weren't processed, function only consumed some, re-run
        run_exe(remainder_exe)
    else:
        # all finished
        pass


def handle_execution_result_output_blocks(
    env: Environment, exe: ExecutableCfg, result: ExecutionResult
):
    # save stream states (latest block, time, schemas?)
    # ignore stream blocks?
    # save table blocks
    pass