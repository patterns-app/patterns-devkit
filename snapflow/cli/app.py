import json
import os
import sys
from importlib import import_module
from typing import Any, List, Optional

import click
import requests
from loguru import logger
from snapflow.core.data_block import DataBlockMetadata
from snapflow.core.environment import Environment, current_env
from snapflow.core.metadata.orm import SNAPFLOW_METADATA_TABLE_PREFIX
from snapflow.core.node import DataBlockLog, SnapLog
from snapflow.core.typing.inference import dict_to_rough_schema
from snapflow.project.project import SNAPFLOW_PROJECT_FILE_NAME, init_project_in_dir
from snapflow.schema.base import schema_to_yaml
from snapflow.utils import common
from snapflow.utils.common import cf
from sqlalchemy import func

REPO_SERVER_API = "http://localhost:8000/components/"  # TODO: configurable

PADDING = " " * 4


def format_line(cols, max_lens):
    s = ""
    for c, m in zip(cols, max_lens):
        fmt = "{0:" + str(m) + "}" + PADDING
        s += fmt.format(str(c))
    return s


def header_line(max_lens):
    return "-" * (sum(max_lens) + (len(max_lens) - 1) * len(PADDING)) + "\n"


def table_pager(headers: List[str], rows: List[List[Any]], show_header: bool = True):
    max_lens = [len(h) for h in headers]
    for row in rows:
        for i, c in enumerate(row):
            max_lens[i] = max(max_lens[i], len(str(c)))
    if show_header:
        yield cf.bold(format_line(headers, max_lens) + "\n")
        yield cf.bold(header_line(max_lens))
    for row in rows:
        yield format_line(row, max_lens) + "\n"


def echo_table(headers, rows):
    pager = table_pager(headers, rows)
    if len(rows) <= 40:
        for line in pager:
            click.echo(line, nl=False)
    else:
        click.echo_via_pager(pager)


class CliAppException(Exception):
    pass


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("-d", "--debug")
@click.option("-m", "--metadata")
@click.pass_context
def app(ctx, debug: bool = False, metadata: Optional[str] = None):
    """Modern Data Pipelines"""
    logger.warning("The snapflow CLI is experimental and not officially supported yet")
    if debug:
        logger.add(sys.stderr, level="DEBUG")
    else:
        logger.add(sys.stderr, level="INFO")
    env = current_env()
    if env is None:
        env = Environment(metadata_storage=metadata)
    logger.info(f"Using environment '{env.metadata_storage.url}'")
    ctx.obj = env


# @click.command()
# @click.option("-c", "--component_type")
# @click.argument("search")
# def search(query: str, component_type: str = None):
#     """Search for components in the snapflow Repository"""
#     params = {"q": query}
#     if component_type:
#         params["component_type"] = component_type
#     resp = requests.get(REPO_SERVER_API + "search", params)
#     resp.raise_for_status()
#     results = resp.json()
#     if not results["results"]:
#         click.secho(f"No results for '{query}'", bold=True)
#         return

#     def result_generator(results):
#         first = True
#         while results["results"]:
#             headers = ["Component type", "Key", "Name"]
#             rows = [
#                 [r["component_type"], r["name"], r["verbose_name"]] for r in results
#             ]
#             for ln in table_pager(headers, rows, show_header=first):
#                 yield ln
#             first = False
#             resp = requests.get(results["next"])
#             resp.raise_for_status()
#             results = resp.json()

#     click.echo_via_pager(result_generator(results))


@click.command()
@click.argument("component_type")
def generate(component_type: str):
    """Generate components from other sources"""
    # TODO: make this a whole wizard flow for each component type
    if component_type == "schema":
        s = ""
        for line in sys.stdin:
            s += line
        schema = dict_to_rough_schema("NewType", json.loads(s))
        schema = schema_to_yaml(schema)
        click.secho("New Schema definition", bold=True)
        click.secho("-----------------------")
        click.echo(schema)
        click.secho("-----------------------")
    else:
        raise CliAppException(f"Invalid component type {component_type}")


@click.command("blocks")
@click.pass_obj
def blocks(env: Environment):
    list_data_blocks(env)


@click.command("nodes")
@click.pass_obj
def nodes(env: Environment):
    list_nodes(env)


def list_data_blocks(env: Environment):
    with env.session_scope() as sess:
        query = (
            sess.query(DataBlockMetadata)
            .filter(~DataBlockMetadata.deleted)
            .order_by(DataBlockMetadata.created_at)
        )
        headers = [
            "ID",
            "Nominal schema",
            "Created by node",
            "# Records",
            "Stored",
        ]
        rows = [
            [
                r.id,
                r.nominal_schema_key,
                r.created_by_node_key,
                r.record_count,
                r.stored_data_blocks.count(),
            ]
            for r in query
        ]
        echo_table(headers, rows)


def list_nodes(env: Environment):
    with env.session_scope() as sess:
        query = (
            sess.query(
                SnapLog.node_key,
                func.count(SnapLog.id),
                func.max(SnapLog.started_at),
                func.count(DataBlockLog.id),
            )
            .join(SnapLog.data_block_logs)
            .group_by(SnapLog.node_key)
            .all()
        )
        headers = [
            "Node key",
            "Run count",
            "Last run at",
            "block count",
        ]
        rows = [(k, c, m.strftime("%F %T")) for k, c, m in query]
    echo_table(headers, rows)


@click.command("logs")
@click.pass_obj
def logs(env: Environment):
    """Show log of Snaps on DataBlocks"""
    with env.session_scope() as sess:
        query = sess.query(SnapLog).order_by(SnapLog.updated_at.desc())
        drls = []
        for dfl in query:
            if dfl.data_block_logs:
                for drl in dfl.data_block_logs:
                    r = [
                        dfl.started_at.strftime("%F %T"),
                        dfl.node_key,
                        drl.direction.display,
                        drl.data_block_id,
                    ]
                    drls.append(r)
            else:
                drls.append(
                    [
                        dfl.started_at.strftime("%F %t"),
                        f"{dfl.node_key} nothing to do",
                        "-",
                        "-",
                    ]
                )
        headers = [
            "Started",
            "_Snap",
            "Direction",
            "DataBlock",
        ]
        echo_table(headers, drls)


@click.command("test")
@click.argument("module")
def test(module: str):
    """Run tests for given module"""
    m = import_module(module)
    m.run_tests()


@click.command("reset")
@click.pass_obj
def reset_metadata(env: Environment):
    """Reset metadata, all or selectively"""
    # TODO
    raise NotImplementedError
    with env.session_scope() as sess:
        sess.execute(
            f"drop table {SNAPFLOW_METADATA_TABLE_PREFIX}snap_log        cascade;"
        )
        sess.execute(
            f"drop table {SNAPFLOW_METADATA_TABLE_PREFIX}snap_log_id_seq cascade;"
        )
        sess.execute(
            f"drop table {SNAPFLOW_METADATA_TABLE_PREFIX}data_resource_log        cascade;"
        )
        sess.execute(
            f"drop table {SNAPFLOW_METADATA_TABLE_PREFIX}data_resource_log_id_seq cascade;"
        )
        sess.execute(
            f"drop table {SNAPFLOW_METADATA_TABLE_PREFIX}data_resource_metadata   cascade;"
        )
        sess.execute(
            f"drop table {SNAPFLOW_METADATA_TABLE_PREFIX}data_set_metadata        cascade;"
        )
        sess.execute(
            f"drop table {SNAPFLOW_METADATA_TABLE_PREFIX}stored_data_resource_metadata cascade;"
        )


@click.command("init")
@click.pass_context
def init_project(ctx: click.Context):
    """Initialize new snapflow project in current dir"""
    curr_dir = os.getcwd()
    try:
        init_project_in_dir(curr_dir)
    except FileExistsError:
        ctx.fail(f"{SNAPFLOW_PROJECT_FILE_NAME} already exists in '{curr_dir}/'")
    click.echo(
        f"Created {SNAPFLOW_PROJECT_FILE_NAME} in '{curr_dir}/'. Edit this file to configure your project."
    )


@click.command("run")
@click.option("-n", "--node", help="Name of node to run (defaults to all)")
@click.option("-D", "--deps", help="Run node's dependencies too. Default False")
@click.option(
    "--once", help="Run each node only once (instead of to exhuastion, the default)"
)
@click.pass_obj
def run(env: Environment, node: str = None, deps: bool = False):
    """Run snapflow pipeline"""
    if node:
        if deps:
            env.produce(node)
        else:
            env.run_node(node)
    else:
        raise NotImplementedError
        env.run_graph()


app.add_command(run)
app.add_command(generate)
app.add_command(logs)
app.add_command(blocks)
app.add_command(nodes)
# app.add_command(search)
app.add_command(reset_metadata)
app.add_command(test)
app.add_command(init_project)
