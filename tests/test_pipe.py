from __future__ import annotations

from typing import Callable

import pytest
from pandas import DataFrame

from dags.core.data_block import DataBlock, DataBlockMetadata
from dags.core.graph import Graph
from dags.core.module import DEFAULT_LOCAL_MODULE_NAME
from dags.core.node import Node, create_node
from dags.core.pipe import Pipe, PipeInterface, PipeLike, pipe
from dags.core.pipe_interface import NodeInterfaceManager, PipeAnnotation
from dags.core.runnable import PipeContext
from dags.core.runtime import RuntimeClass
from dags.core.sql.pipe import sql_pipe
from dags.modules import core
from dags.utils.typing import T, U
from tests.utils import (
    TestSchema1,
    make_test_env,
    make_test_execution_context,
    pipe_chain_t1_to_t2,
    pipe_generic,
    pipe_self,
    pipe_t1_sink,
    pipe_t1_source,
    pipe_t1_to_t2,
)


@pytest.mark.parametrize(
    "annotation,expected",
    [
        (
            "DataBlock[Type]",
            PipeAnnotation(
                data_format_class="DataBlock",
                schema_like="Type",
                # is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataBlock[Type]",
            ),
        ),
        (
            "DataSet[Type]",
            PipeAnnotation(
                data_format_class="DataSet",
                schema_like="Type",
                # is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataSet[Type]",
            ),
        ),
        (
            "DataBlock[T]",
            PipeAnnotation(
                data_format_class="DataBlock",
                schema_like="T",
                # is_iterable=False,
                is_generic=True,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataBlock[T]",
            ),
        ),
    ],
)
def test_typed_annotation(annotation: str, expected: PipeAnnotation):
    tda = PipeAnnotation.from_type_annotation(annotation)
    assert tda == expected


def pipe_notworking(_1: int, _2: str, input: DataBlock[TestSchema1]):
    # Bad args
    pass


def df4(
    input: DataBlock[T],
    dr2: DataBlock[U],
    dr3: DataBlock[U],
) -> DataFrame[T]:
    pass


@pytest.mark.parametrize(
    "pipe,expected",
    [
        (
            pipe_t1_sink,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        schema_like="TestSchema1",
                        name="input",
                        # is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[TestSchema1]",
                    )
                ],
                output=None,
                requires_pipe_context=True,
            ),
        ),
        (
            pipe_t1_to_t2,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        schema_like="TestSchema1",
                        name="input",
                        # is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[TestSchema1]",
                    )
                ],
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    schema_like="TestSchema2",
                    # is_iterable=False,
                    is_generic=False,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[TestSchema2]",
                ),
                requires_pipe_context=False,
            ),
        ),
        (
            pipe_generic,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        schema_like="T",
                        name="input",
                        # is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[T]",
                    )
                ],
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    schema_like="T",
                    # is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_pipe_context=False,
            ),
        ),
        (
            pipe_self,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        schema_like="T",
                        name="input",
                        # is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        is_self_ref=False,
                        original_annotation="DataBlock[T]",
                    ),
                    PipeAnnotation(
                        data_format_class="DataBlock",
                        schema_like="T",
                        name="this",
                        # is_iterable=False,
                        is_generic=True,
                        is_optional=True,
                        is_variadic=False,
                        is_self_ref=True,
                        original_annotation="DataBlock[T]",
                    ),
                ],
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    schema_like="T",
                    # is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_pipe_context=False,
            ),
        ),
    ],
)
def test_pipe_interface(pipe: PipeLike, expected: PipeInterface):
    env = make_test_env()
    g = Graph(env)
    if isinstance(pipe, Pipe):
        val = pipe.get_interface(env)
    elif isinstance(pipe, Callable):
        val = PipeInterface.from_pipe_definition(pipe)
    else:
        raise
    assert val == expected
    node = create_node(g, "_test", pipe, inputs="mock")
    assert node.get_interface() == expected


def test_declared_schema_mapping():
    ec = make_test_execution_context()
    env = ec.env
    g = Graph(env)
    mapping = {"f1": "mapped_f1"}
    n1 = g.add_node("node1", pipe_t1_to_t2, schema_mapping=mapping)
    pi = n1.get_interface()
    im = NodeInterfaceManager(ctx=ec, node=n1)
    block = DataBlockMetadata(
        expected_schema_key="_test.TestSchema1",
        realized_schema_key="_test.TestSchema1",
    )
    bpi = im.get_bound_interface({"input": block})
    assert len(bpi.inputs) == 1
    input = bpi.inputs[0]
    schema_mapping = input.get_schema_mapping(env)
    assert schema_mapping.as_dict() == mapping


def test_natural_schema_mapping():
    # TODO
    ec = make_test_execution_context()
    env = ec.env
    g = Graph(env)
    mapping = {"f1": "mapped_f1"}
    n1 = g.add_node("node1", pipe_t1_to_t2, schema_mapping=mapping)
    pi = n1.get_interface()
    im = NodeInterfaceManager(ctx=ec, node=n1)
    block = DataBlockMetadata(
        expected_schema_key="_test.TestSchema1",
        realized_schema_key="_test.TestSchema1",
    )
    bpi = im.get_bound_interface({"input": block})
    assert len(bpi.inputs) == 1
    input = bpi.inputs[0]
    schema_mapping = input.get_schema_mapping(env)
    assert schema_mapping.as_dict() == mapping


def test_inputs():
    env = make_test_env()
    g = Graph(env)
    g.add_node("node1", pipe_t1_source)
    n2 = g.add_node("node2", pipe_t1_to_t2, inputs={"input": "node1"})
    dfi = n2.get_interface()
    assert dfi is not None
    n3 = g.add_node("node3", pipe_chain_t1_to_t2, inputs="node1")
    dfi = n3.get_interface()
    assert dfi is not None


def test_python_pipe():
    env = make_test_env()
    p = pipe(pipe_t1_sink)
    assert (
        p.name == pipe_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "name1"
    p = pipe(pipe_t1_sink, name=k)
    assert p.name == k
    assert p.key == f"{DEFAULT_LOCAL_MODULE_NAME}.{k}"

    pi = p.get_interface(env)
    assert pi is not None


@pipe("k1", compatible_runtimes="python")
def df1():
    pass


@pipe("k1", compatible_runtimes="mysql")
def df2():
    pass


def test_node_no_inputs():
    env = make_test_env()
    g = Graph(env)
    df = pipe(pipe_t1_source)
    node1 = create_node(g, "node1", df)
    assert {node1: node1}[node1] is node1  # Test hash
    dfi = node1.get_interface()
    assert dfi.inputs == []
    assert dfi.output is not None
    assert node1.get_declared_inputs() == {}


def test_node_inputs():
    env = make_test_env()
    g = Graph(env)
    df = pipe(pipe_t1_source)
    node = create_node(g, "node", df)
    df = pipe(pipe_t1_sink)
    with pytest.raises(Exception):
        # Bad input
        create_node(g, "node_fail", df, input="Turname")  # type: ignore
    node1 = create_node(g, "node1", df, inputs=node)
    dfi = node1.get_interface()
    dfi = node1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is None
    assert list(node1.get_declared_inputs().keys()) == ["input"]
    # assert node1.get_input("input").get_upstream(env)[0] is node


def test_node_config():
    env = make_test_env()
    g = Graph(env)
    config_vals = []

    def pipe_ctx(ctx: PipeContext):
        config_vals.append(ctx.get_config_value("test"))

    n = g.add_node("ctx", pipe_ctx, config={"test": 1, "extra_arg": 2})
    with env.execution(g) as exe:
        exe.run(n)
    assert config_vals == [1]


def test_any_schema_interface():
    env = make_test_env()
    env.add_module(core)

    def pipe_any(input: DataBlock) -> DataFrame:
        pass

    df = pipe(pipe_any)
    dfi = df.get_interface(env)
    assert dfi.inputs[0].schema_like == "Any"
    assert dfi.output.schema_like == "Any"
