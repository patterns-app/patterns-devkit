from __future__ import annotations

from datetime import datetime

from basis import Record, Stream, Table, basis
from basis.core.block import SelfReference, Stream
from basis.core.function import function
from basis.utils.typing import T
from pandas import DataFrame, concat

Context = TypeVar("Context")


@basis(
    display_name="Aggregator (Python)",
    namespace="core",
    inputs=[Record("e", stream=True), Table("aggregate")],
    outputs=[Table("aggregate")],  # Defaults to "stdout"
    # parameters=[Int("count"), DateTime("start_date"), Text("name")],
)
def aggregator(ctx: Context, p1: int):
    """
    Special function that aggregates a stream of Records into a Table

    Inputs:
        next: record stream to aggregate into table
        agg: previous table to add record to (recursive input)
    """
    agg_records = ctx.get_table("aggregate").as_json()
    consumed = []
    for nxt in ctx.get_next_record("e", stream=True):
        nxt_records = nxt.as_json()
        if not_valid(nxt_records):
            ctx.emit_error(nxt_records, "Invalid record")
        agg_records.append(nxt_records)
        consumed.append(nxt)
    if consumed:
        ctx.emit_table(agg_records)
        ctx.consume("e", consumed)


@basis(
    display_name="Streaming function",
    namespace="core",
    inputs=[Record("event")],
    outputs=[Record()],  # Defaults to "stdout"
)
def stream_fn(ctx: Context, param1: int, param2: datetime):
    evnt = ctx.get_record("event").as_json()
    new_evnt = do_something_with_record(evnt, param1, param2)
    ctx.emit_record(new_evnt)
    ctx.log_latest_record_consumed("event", evnt)


@basis(
    display_name="Importer",
    namespace="core",
    outputs=[Record("customers"), Record("orders")],  # Defaults to "stdout"
)
def importer(ctx: Context, api_key: str, start: datetime):
    state = ctx.get_state()
    latest_record_id = state.get("latest_record_id")
    data = reqeusts.get(url, api_key=api_key, after=latest_record_id).json()
    for record in data:
        if record["type"] == "Customer":
            ctx.emit_record(record, "customers")
        elif record["type"] == "Order":
            ctx.emit_record(record, "orders")
        else:
            ctx.emit_error(record, f"Unknown object type {record['type']}")
        latest_record_id = record["id"]
    ctx.emit_state({"latest_record_id": latest_record_id})


# Simple functions for end-user (if not building re-usable or complex components)


@function  # "Generic" version that just mimics input
def simple_record_fn(record, optional_table1=None):
    return record


@record_function
def simple_record_fn(record, optional_table1=None):
    record["ssn"] = get_ssn(record["full_name"])
    return record


@table_function
def simple_table_fn(table1, table2):
    return table1.as_pandas().merge(table2.as_pandas())
