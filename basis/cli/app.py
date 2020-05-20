import json
import sys
from typing import Any, List

import click
import requests
from sqlalchemy import func

from basis.core.data_function import DataFunctionLog
from basis.core.data_resource import DataResourceMetadata, DataSetMetadata
from basis.core.environment import current_env
from basis.core.object_type import dict_to_rough_otype, otype_to_yaml
from basis.utils.common import cf, cycle_colors_unique

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
def app():
    pass


@click.command()
@click.option("-c", "--component_type")
@click.argument("query")
def search(query: str, component_type: str = None):
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
            for l in table_pager(headers, rows, show_header=first):
                yield l
            first = False
            resp = requests.get(results["next"])
            resp.raise_for_status()
            results = resp.json()

    click.echo_via_pager(result_generator(results))


@click.command()
@click.argument("component_type")
def generate(component_type: str):
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
def list_component(component_type):
    if component_type == "dataresources":
        list_data_resources()
    elif component_type == "datasets":
        list_data_sets()
    elif component_type == "datafunctions":
        list_data_functions()
    else:
        click.echo(f"Unknown component type {component_type}")


def list_data_resources():
    env = current_env()  # TODO inject this via optional arg too -e --env
    with env.session_scope() as sess:
        query = (
            sess.query(DataResourceMetadata)
            .filter(~DataResourceMetadata.deleted)
            .order_by(DataResourceMetadata.created_at)
        )
        headers = ["ID", "BaseType", "Create by node", "Stored"]
        rows = [
            [r.id, r.otype_uri, r.created_by(sess), r.stored_data_resources.count()]
            for r in query
        ]
        echo_table(headers, rows)


def list_data_sets():
    env = current_env()
    with env.session_scope() as sess:
        query = sess.query(DataSetMetadata).order_by(DataSetMetadata.created_at)
        headers = ["Key", "BaseType", "Stored"]
        rows = [
            [
                r.key,
                r.data_resource.otype_uri,
                r.data_resource.stored_data_resources.count(),
            ]
            for r in query
        ]
    echo_table(headers, rows)


def list_data_functions():
    env = current_env()
    with env.session_scope() as sess:
        query = (
            sess.query(
                DataFunctionLog.configured_data_function_key,
                func.count(DataFunctionLog.id),
                func.max(DataFunctionLog.started_at),
            )
            .group_by(DataFunctionLog.configured_data_function_key)
            .all()
        )
        headers = [
            "Key",
            "Run count",
            "Last run at",
        ]
        rows = [(k, c, m.strftime("%F %T")) for k, c, m in query]
    echo_table(headers, rows)


@click.command("log")
def show_log():
    env = current_env()
    with env.session_scope() as sess:
        query = sess.query(DataFunctionLog).order_by(DataFunctionLog.updated_at.desc())
        drls = []
        for dfl in query:
            if dfl.data_resource_logs:
                for drl in dfl.data_resource_logs:
                    r = [
                        dfl.started_at.strftime("%F %T"),
                        dfl.configured_data_function_key,
                        drl.direction.display,
                        cycle_colors_unique(drl.data_resource_id),
                    ]
                    drls.append(r)
            else:
                drls.append(
                    [
                        dfl.started_at.strftime("%F %t"),
                        f"{dfl.configured_data_function_key} nothing to do",
                        "-",
                        "-",
                    ]
                )
        headers = [
            "Started",
            "Function",
            "Direction",
            "DataResource",
        ]
        echo_table(headers, drls)


app.add_command(generate)
app.add_command(show_log)
app.add_command(list_component)
app.add_command(search)
