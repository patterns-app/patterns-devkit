import json
import os
import sys
from importlib import import_module
from typing import Any, List

import click
import requests
from sqlalchemy import func

from basis.core.data_block import DataBlockMetadata, DataSetMetadata
from basis.core.environment import Environment, current_env
from basis.core.function_node import DataFunctionLog
from basis.core.typing.inference import dict_to_rough_otype
from basis.core.typing.object_type import otype_to_yaml
from basis.project.project import BASIS_PROJECT_FILE_NAME, init_project_in_dir
from basis.utils import common
from basis.utils.common import cf, cycle_colors_unique
from loguru import logger

REPO_SERVER_API = "http://localhost:8000/components/"  # TODO: configurable

PADDING = " " * 4


def format_line(cols, max_lens):
    s = ""
    for c, m in zip(cols, max_lens):
        fmt = "{0:" + str(m) + "}" + PADDING
        s += fmt.format(c)
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


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("-d", "--debug")
@click.pass_context
def app(ctx, debug: bool = False):
    """Modern Data Pipelines"""
    logger.enable("basis")
    if debug:
        logger.add(sys.stderr, level="DEBUG")
    else:
        logger.add(sys.stderr, level="INFO")
    # TODO some way to pass in env
    env = Environment()
    try:
        env = current_env()
    except:
        pass
    ctx.obj = env


@click.command()
@click.option("-c", "--component_type")
@click.argument("search")
def search(query: str, component_type: str = None):
    """Search for components in the Basis Repository"""
    params = {"q": query}
    if component_type:
        params["component_type"] = component_type
    resp = requests.get(REPO_SERVER_API + "search", params)
    resp.raise_for_status()
    results = resp.json()
    if not results["results"]:
        click.secho(f"No results for '{query}'", bold=True)
        return

    def result_generator(results):
        first = True
        while results["results"]:
            headers = ["Component type", "Key", "Name"]
            rows = [[r["component_type"], r["key"], r["verbose_name"]] for r in results]
            for ln in table_pager(headers, rows, show_header=first):
                yield ln
            first = False
            resp = requests.get(results["next"])
            resp.raise_for_status()
            results = resp.json()

    click.echo_via_pager(result_generator(results))


@click.command()
@click.argument("component_type")
def generate(component_type: str):
    """Generate components from other sources"""
    # TODO: make this a whole wizard flow for each component type
    if component_type == "otype":
        s = ""
        for line in sys.stdin:
            s += line
        otype = dict_to_rough_otype("NewType", json.loads(s))
        otype = otype_to_yaml(otype)
        click.secho("New ObjectType definition", bold=True)
        click.secho("-----------------------")
        click.echo(otype)
        click.secho("-----------------------")


@click.command("list")
@click.argument("component_type")
@click.pass_obj
def list_component(env: Environment, component_type):
    """List components in environment"""
    if component_type == "datablocks":
        list_data_blocks(env)
    elif component_type == "datasets":
        list_data_sets(env)
    elif component_type == "functions":
        list_data_functions(env)
    else:
        click.echo(f"Unknown component type {component_type}")


def list_data_blocks(env: Environment):
    with env.session_scope() as sess:
        query = (
            sess.query(DataBlockMetadata)
            .filter(~DataBlockMetadata.deleted)
            .order_by(DataBlockMetadata.created_at)
        )
        headers = ["ID", "BaseType", "Create by node", "Stored"]
        rows = [
            [
                r.id,
                r.expected_otype_uri,
                r.created_by(sess),
                r.stored_data_blocks.count(),
            ]
            for r in query
        ]
        echo_table(headers, rows)


def list_data_sets(env: Environment):
    with env.session_scope() as sess:
        query = sess.query(DataSetMetadata).order_by(DataSetMetadata.created_at)
        headers = ["Name", "BaseType", "Stored"]
        rows = [
            [
                r.name,
                r.data_block.expected_otype_uri,
                r.data_block.stored_data_blocks.count(),
            ]
            for r in query
        ]
    echo_table(headers, rows)


def list_data_functions(env: Environment):
    with env.session_scope() as sess:
        query = (
            sess.query(
                DataFunctionLog.function_node_name,
                func.count(DataFunctionLog.id),
                func.max(DataFunctionLog.started_at),
            )
            .group_by(DataFunctionLog.function_node_name)
            .all()
        )
        headers = [
            "Name",
            "Run count",
            "Last run at",
        ]
        rows = [(k, c, m.strftime("%F %T")) for k, c, m in query]
    echo_table(headers, rows)


@click.command("log")
@click.pass_obj
def show_log(env: Environment):
    """Show log of DataFunctions on DataBlocks"""
    with env.session_scope() as sess:
        query = sess.query(DataFunctionLog).order_by(DataFunctionLog.updated_at.desc())
        drls = []
        for dfl in query:
            if dfl.data_block_logs:
                for drl in dfl.data_block_logs:
                    r = [
                        dfl.started_at.strftime("%F %T"),
                        dfl.function_node_name,
                        drl.direction.display,
                        cycle_colors_unique(drl.data_block_id),
                    ]
                    drls.append(r)
            else:
                drls.append(
                    [
                        dfl.started_at.strftime("%F %t"),
                        f"{dfl.function_node_name} nothing to do",
                        "-",
                        "-",
                    ]
                )
        headers = [
            "Started",
            "Function",
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
        sess.execute("drop table basis_data_function_log        cascade;")
        sess.execute("drop table basis_data_function_log_id_seq cascade;")
        sess.execute("drop table basis_data_resource_log        cascade;")
        sess.execute("drop table basis_data_resource_log_id_seq cascade;")
        sess.execute("drop table basis_data_resource_metadata   cascade;")
        sess.execute("drop table basis_data_set_metadata        cascade;")
        sess.execute("drop table basis_stored_data_resource_metadata cascade;")


@click.command("init")
@click.pass_context
def init_project(ctx: click.Context):
    """Initialize new Basis project in current dir"""
    curr_dir = os.getcwd()
    try:
        init_project_in_dir(curr_dir)
    except FileExistsError:
        ctx.fail(f"{BASIS_PROJECT_FILE_NAME} already exists in {curr_dir}")
    click.echo(
        f"Created {BASIS_PROJECT_FILE_NAME} in {curr_dir}. Edit this file to configure your project."
    )


@click.command("run")
@click.option("-n", "--node", help="Name of node to run (defaults to all)")
@click.option("-D", "--deps", help="Run node's dependencies too. Default False")
@click.option(
    "--once", help="Run each node only once (instead of to exhuastion, the default)"
)
@click.pass_obj
def run(env: Environment, node: str = None, deps: bool = False):
    """Run Basis pipeline"""
    if node:
        if deps:
            env.produce(node)
        else:
            env.update(node)
    else:
        env.update_all()


# TODO: create new blank component (how diff from generate? Rename?)
# @click.command("create")
# @click.pass_obj
# def create_component(env: Environment):
#       pass


app.add_command(run)
app.add_command(generate)
app.add_command(show_log)
app.add_command(list_component)
app.add_command(search)
app.add_command(reset_metadata)
app.add_command(test)
app.add_command(init_project)
