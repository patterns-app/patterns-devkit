from __future__ import annotations

from typing import Callable

import pytest
from pandas import DataFrame
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.graph import Graph, graph
from snapflow.core.module import DEFAULT_LOCAL_MODULE_NAME
from snapflow.core.node import DeclaredNode, node
from snapflow.core.pipe import Pipe, PipeInterface, PipeLike, pipe
from snapflow.core.pipe_interface import (
    NodeInterfaceManager,
    PipeAnnotation,
    get_schema_translation,
    make_default_output_annotation,
)
from snapflow.core.runnable import PipeContext
from snapflow.core.streams import block_as_stream
from snapflow.modules import core
from snapflow.utils.typing import T, U
from tests.utils import (
    TestSchema1,
    make_test_env,
    make_test_execution_context,
    pipe_chain_t1_to_t2,
    pipe_generic,
    pipe_self,
    pipe_stream,
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
                is_stream=False,
                original_annotation="DataBlock[Type]",
            ),
        ),
        (
            "DataFrame[Type]",
            PipeAnnotation(
                data_format_class="DataFrame",
                schema_like="Type",
                # is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                is_stream=False,
                original_annotation="DataFrame[Type]",
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
                is_stream=False,
                original_annotation="DataBlock[T]",
            ),
        ),
        (
            "DataBlockStream[Type]",
            PipeAnnotation(
                data_format_class="DataBlockStream",
                schema_like="Type",
                # is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                is_stream=True,
                original_annotation="DataBlockStream[Type]",
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
                output=make_default_output_annotation(),
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
    if isinstance(pipe, Pipe):
        val = pipe.get_interface()
    elif isinstance(pipe, Callable):
        val = PipeInterface.from_pipe_definition(pipe)
    else:
        raise
    assert val == expected
    node = DeclaredNode(key="_test", pipe=pipe, upstream="mock").instantiate(env)
    assert node.get_interface() == expected


def test_generic_schema_resolution():
    ec = make_test_execution_context()
    env = ec.env
    g = Graph(env)
    n1 = g.create_node(key="node1", pipe=pipe_generic, upstream="n0")
    # pi = n1.get_interface()
    im = NodeInterfaceManager(ctx=ec, node=n1)
    block = DataBlockMetadata(
        nominal_schema_key="_test.TestSchema1",
        realized_schema_key="_test.TestSchema2",
    )
    env.session.add(block)
    env.session.flush([block])
    stream = block_as_stream(block, ec)
    bi = im.get_bound_interface({"input": stream})
    assert len(bi.inputs) == 1
    assert bi.resolve_nominal_output_schema(env) is TestSchema1


def test_declared_schema_translation():
    ec = make_test_execution_context()
    env = ec.env
    g = Graph(env)
    translation = {"f1": "mapped_f1"}
    n1 = g.create_node(
        key="node1", pipe=pipe_t1_to_t2, upstream="n0", schema_translation=translation
    )
    pi = n1.get_interface()
    # im = NodeInterfaceManager(ctx=ec, node=n1)
    block = DataBlockMetadata(
        nominal_schema_key="_test.TestSchema1",
        realized_schema_key="_test.TestSchema1",
    )
    # stream = block_as_stream(block, ec, pi.inputs[0].schema(env), translation)
    # bi = im.get_bound_stream_interface({"input": stream})
    # assert len(bi.inputs) == 1
    # input: StreamInput = bi.inputs[0]
    schema_translation = get_schema_translation(
        env,
        block,
        declared_schema=pi.inputs[0].schema(env),
        declared_schema_translation=translation,
    )
    assert schema_translation.as_dict() == translation


def test_natural_schema_translation():
    # TODO
    ec = make_test_execution_context()
    env = ec.env
    g = Graph(env)
    translation = {"f1": "mapped_f1"}
    n1 = g.create_node(
        key="node1", pipe=pipe_t1_to_t2, upstream="n0", schema_translation=translation
    )
    pi = n1.get_interface()
    # im = NodeInterfaceManager(ctx=ec, node=n1)
    block = DataBlockMetadata(
        nominal_schema_key="_test.TestSchema1",
        realized_schema_key="_test.TestSchema1",
    )
    schema_translation = get_schema_translation(
        env,
        block,
        declared_schema=pi.inputs[0].schema(env),
        declared_schema_translation=translation,
    )
    assert schema_translation.as_dict() == translation
    # bpi = im.get_bound_stream_interface({"input": block})
    # assert len(bpi.inputs) == 1
    # input = bpi.inputs[0]
    # schema_translation = input.get_schema_translation(env)
    # assert schema_translation.as_dict() == translation


def test_inputs():
    env = make_test_env()
    g = Graph(env)
    g.create_node(key="node1", pipe=pipe_t1_source)
    n2 = g.create_node(key="node2", pipe=pipe_t1_to_t2, upstream={"input": "node1"})
    pi = n2.get_interface()
    assert pi is not None
    n3 = g.create_node(key="node3", pipe=pipe_chain_t1_to_t2, upstream="node1")
    pi = n3.get_interface()
    assert pi is not None


def test_python_pipe():
    p = pipe(pipe_t1_sink)
    assert (
        p.name == pipe_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "name1"
    p = pipe(pipe_t1_sink, name=k)
    assert p.name == k
    assert p.key == f"{DEFAULT_LOCAL_MODULE_NAME}.{k}"

    pi = p.get_interface()
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
    node1 = g.create_node(key="node1", pipe=df)
    assert {node1: node1}[node1] is node1  # Test hash
    pi = node1.get_interface()
    assert pi.inputs == []
    assert pi.output is not None
    assert node1.declared_inputs == {}


def test_node_inputs():
    env = make_test_env()
    g = Graph(env)
    df = pipe(pipe_t1_source)
    node = g.create_node(key="node", pipe=df)
    df = pipe(pipe_t1_sink)
    node1 = g.create_node(key="node1", pipe=df, upstream=node)
    pi = node1.get_interface()
    assert len(pi.inputs) == 1
    assert pi.output == make_default_output_annotation()
    assert list(node1.declared_inputs.keys()) == ["input"]
    # assert node1.get_input("input").get_upstream(env)[0] is node


def test_node_stream_inputs():
    pi = pipe(pipe_stream).get_interface()
    assert len(pi.inputs) == 1
    assert pi.inputs[0].is_stream


def test_node_config():
    env = make_test_env()
    g = Graph(env)
    config_vals = []

    def pipe_ctx(ctx: PipeContext):
        config_vals.append(ctx.get_config_value("test"))

    n = g.create_node(key="ctx", pipe=pipe_ctx, config={"test": 1, "extra_arg": 2})
    with env.execution(g) as exe:
        exe.run(n)
    assert config_vals == [1]


def test_any_schema_interface():
    env = make_test_env()
    env.add_module(core)

    def pipe_any(input: DataBlock) -> DataFrame:
        pass

    df = pipe(pipe_any)
    pi = df.get_interface()
    assert pi.inputs[0].schema_like == "Any"
    assert pi.output.schema_like == "Any"
