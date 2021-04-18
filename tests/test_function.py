from __future__ import annotations

from typing import Callable

import pytest
from pandas import DataFrame
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.execution import FunctionContext
from snapflow.core.execution.executable import Executable, ExecutableConfiguration
from snapflow.core.function import (
    DeclaredFunctionInterface,
    Function,
    FunctionLike,
    Input,
    Output,
    Param,
    Parameter,
    _Function,
)
from snapflow.core.function_interface import (
    DeclaredInput,
    DeclaredOutput,
    NodeInterfaceManager,
    ParsedAnnotation,
    function_interface_from_callable,
    get_schema_translation,
    make_default_output,
    parse_annotation,
)
from snapflow.core.graph import Graph, graph
from snapflow.core.module import DEFAULT_LOCAL_NAMESPACE
from snapflow.core.node import DeclaredNode, node
from snapflow.core.streams import StreamBuilder, block_as_stream
from snapflow.modules import core
from snapflow.utils.typing import T, U
from tests.utils import (
    TestSchema1,
    function_chain_t1_to_t2,
    function_generic,
    function_multiple_input,
    function_self,
    function_stream,
    function_t1_sink,
    function_t1_source,
    function_t1_to_t2,
    make_test_env,
    make_test_run_context,
)

context_input = DeclaredInput(
    name="ctx",
    schema_like="Any",
    data_format="FunctionContext",
    reference=False,
    _required=True,
    from_self=False,
    stream=False,
    context=True,
)


@pytest.mark.parametrize(
    "annotation,expected",
    [
        (
            "DataBlock[Type]",
            ParsedAnnotation(
                data_format_class="DataBlock",
                schema_like="Type",
                is_optional=False,
                original_annotation="DataBlock[Type]",
            ),
        ),
        (
            "Optional[DataBlock[Type]]",
            ParsedAnnotation(
                data_format_class="DataBlock",
                schema_like="Type",
                is_optional=True,
                original_annotation="Optional[DataBlock[Type]]",
            ),
        ),
        (
            "DataFrame[Type]",
            ParsedAnnotation(
                data_format_class="DataFrame",
                schema_like="Type",
                is_optional=False,
                original_annotation="DataFrame[Type]",
            ),
        ),
        (
            "DataBlock[T]",
            ParsedAnnotation(
                data_format_class="DataBlock",
                schema_like="T",
                is_optional=False,
                original_annotation="DataBlock[T]",
            ),
        ),
        (
            "DataBlockStream[Type]",
            ParsedAnnotation(
                data_format_class="DataBlockStream",
                schema_like="Type",
                is_optional=False,
                original_annotation="DataBlockStream[Type]",
            ),
        ),
    ],
)
def test_typed_annotation(annotation: str, expected: ParsedAnnotation):
    tda = parse_annotation(annotation)
    assert tda == expected


def function_notworking(_1: int, _2: str, input: DataBlock[TestSchema1]):
    # Bad args
    pass


def df4(
    input: DataBlock[T],
    dr2: DataBlock[U],
    dr3: DataBlock[U],
) -> DataFrame[T]:
    pass


@pytest.mark.parametrize(
    "function_like,expected",
    [
        (
            function_t1_sink,
            DeclaredFunctionInterface(
                inputs=[
                    DeclaredInput(
                        data_format="DataBlock",
                        schema_like="TestSchema1",
                        name="input",
                        _required=True,
                    ),
                ],
                output=make_default_output(),
                context=context_input,
            ),
        ),
        (
            function_t1_to_t2,
            DeclaredFunctionInterface(
                inputs=[
                    DeclaredInput(
                        data_format="DataBlock",
                        schema_like="TestSchema1",
                        name="input",
                        _required=True,
                    ),
                ],
                output=DeclaredOutput(
                    data_format="DataFrame",
                    schema_like="TestSchema2",
                ),
            ),
        ),
        (
            function_generic,
            DeclaredFunctionInterface(
                inputs=[
                    DeclaredInput(
                        data_format="DataBlock",
                        schema_like="T",
                        name="input",
                        _required=True,
                    ),
                ],
                output=DeclaredOutput(
                    data_format="DataFrame",
                    schema_like="T",
                ),
            ),
        ),
        (
            function_self,
            DeclaredFunctionInterface(
                inputs=[
                    DeclaredInput(
                        data_format="DataBlock",
                        schema_like="T",
                        name="input",
                        _required=True,
                    ),
                    DeclaredInput(
                        data_format="DataBlock",
                        schema_like="T",
                        name="this",
                        _required=False,
                        from_self=True,
                        reference=True,
                    ),
                ],
                output=DeclaredOutput(
                    data_format="DataFrame",
                    schema_like="T",
                ),
            ),
        ),
    ],
)
def test_function_interface(
    function_like: FunctionLike, expected: DeclaredFunctionInterface
):
    p = Function(function_like)
    val = p.get_interface()
    assert set(val.inputs) == set(expected.inputs)
    assert val.output == expected.output
    # node = DeclaredNode(key="_test", function=function, inputs={"input": "mock"}).instantiate(
    #     env
    # )
    # assert node.get_interface() == expected


def test_generic_schema_resolution():
    ec = make_test_run_context()
    env = ec.env
    g = Graph(env)
    n1 = g.create_node(key="node1", function=function_generic, input="n0")
    # pi = n1.get_interface()
    with env.md_api.begin():
        exe = Executable(node=n1, function=n1.function, execution_context=ec)
        im = NodeInterfaceManager(exe)
        block = DataBlockMetadata(
            nominal_schema_key="_test.TestSchema1",
            realized_schema_key="_test.TestSchema2",
        )
        env.md_api.add(block)
        env.md_api.flush([block])
        stream = block_as_stream(block, ec)
        bi = im.get_bound_interface({"input": stream})
        assert len(bi.inputs) == 1
        assert bi.resolve_nominal_output_schema(env) is TestSchema1


def test_declared_schema_translation():
    ec = make_test_run_context()
    env = ec.env
    g = Graph(env)
    translation = {"f1": "mapped_f1"}
    n1 = g.create_node(
        key="node1",
        function=function_t1_to_t2,
        input="n0",
        schema_translation=translation,
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
    with env.md_api.begin():
        schema_translation = get_schema_translation(
            env,
            block.realized_schema(env),
            target_schema=env.get_schema(pi.inputs[0].schema_like),
            declared_schema_translation=translation,
        )
        assert schema_translation.as_dict() == translation


def test_natural_schema_translation():
    # TODO
    ec = make_test_run_context()
    env = ec.env
    g = Graph(env)
    translation = {"f1": "mapped_f1"}
    n1 = g.create_node(
        key="node1",
        function=function_t1_to_t2,
        input="n0",
        schema_translation=translation,
    )
    pi = n1.get_interface()
    # im = NodeInterfaceManager(ctx=ec, node=n1)
    block = DataBlockMetadata(
        nominal_schema_key="_test.TestSchema1",
        realized_schema_key="_test.TestSchema1",
    )
    with env.md_api.begin():
        schema_translation = get_schema_translation(
            env,
            block.realized_schema(env),
            target_schema=env.get_schema(pi.inputs[0].schema_like),
            declared_schema_translation=translation,
        )
        assert schema_translation.as_dict() == translation
    # bpi = im.get_bound_stream_interface({"input": block})
    # assert len(bpi.inputs) == 1
    # input = bpi.inputs[0]
    # schema_translation = input.get_schema_translation(env)
    # assert schema_translation.as_dict() == translation


def test_inputs():
    ec = make_test_run_context()
    env = ec.env
    g = graph()
    n1 = g.create_node(function=function_t1_source)
    n2 = g.create_node(function=function_t1_to_t2, inputs={"input": n1})
    pi = n2.instantiate(env).get_interface()
    assert pi is not None
    n4 = g.create_node(function=function_multiple_input)
    n4.set_inputs({"input": n1})
    pi = n4.instantiate(env).get_interface()
    assert pi is not None

    # ec.graph = g.instantiate(env)
    n1 = n1.instantiate(env)
    n4 = n4.instantiate(env)
    with env.md_api.begin():
        exe = Executable(node=n1, function=n1.function, execution_context=ec)
        im = NodeInterfaceManager(exe)
        bi = im.get_bound_interface()
        assert bi is not None
        exe = Executable(node=n4, function=n4.function, execution_context=ec)
        im = NodeInterfaceManager(exe)
        db = DataBlockMetadata(
            nominal_schema_key="_test.TestSchema1",
            realized_schema_key="_test.TestSchema1",
        )
        env.md_api.add(db)
        bi = im.get_bound_interface({"input": StreamBuilder().as_managed_stream(ec)})
        assert bi is not None


def test_python_function():
    p = Function(function_t1_sink)
    assert (
        p.name == function_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "name1"
    p = Function(function_t1_sink, name=k)
    assert p.name == k
    assert p.key == f"{DEFAULT_LOCAL_NAMESPACE}.{k}"

    pi = p.get_interface()
    assert pi is not None


@Function("k1")
def df1():
    pass


@Function("k1", required_storage_classes=["database"])
def df2():
    pass


def test_node_no_inputs():
    env = make_test_env()
    g = Graph(env)
    df = Function(function_t1_source)
    node1 = g.create_node(key="node1", function=df)
    assert {node1: node1}[node1] is node1  # Test hash
    pi = node1.get_interface()
    assert pi.inputs == []
    assert pi.output is not None
    assert node1.declared_inputs == {}


def test_node_inputs():
    env = make_test_env()
    g = Graph(env)
    df = Function(function_t1_source)
    node = g.create_node(key="node", function=df)
    df = Function(function_t1_sink)
    node1 = g.create_node(key="node1", function=df, input=node)
    pi = node1.get_interface()
    assert len(pi.inputs) == 1
    assert pi.output == make_default_output()
    assert list(node1.declared_inputs.keys()) == ["input"]


def test_node_stream_inputs():
    pi = Function(function_stream).get_interface()
    assert len(pi.inputs) == 1
    assert pi.inputs[0].stream


def test_node_params():
    env = make_test_env()
    g = Graph(env)
    param_vals = []

    @Param("test", "str")
    def function_ctx(ctx: FunctionContext):
        param_vals.append(ctx.get_param("test"))

    n = g.create_node(
        key="ctx", function=function_ctx, params={"test": 1, "extra_arg": 2}
    )
    env.run_node(n, g)
    assert param_vals == [1]


def test_any_schema_interface():
    env = make_test_env()
    env.add_module(core)

    def function_any(input: DataBlock) -> DataFrame:
        pass

    df = Function(function_any)
    pi = df.get_interface()
    assert pi.inputs[0].schema_like == "Any"
    assert pi.output.schema_like == "Any"


def test_api():
    @Function(namespace="module1")
    @Param("p1", "str")
    @Input("i1")
    def s1(ctx: FunctionContext):
        pass

    @Param("p1", "str")
    @Function("s1", namespace="module1")
    def s2(ctx: FunctionContext, i1: DataBlock):
        pass

    @Function("s1")
    @Param("p1", "str")
    @Input("i1")
    def s3(ctx: FunctionContext, i1: DataBlock):
        pass

    @Output(schema="Any")
    @Function(name="s1", params=[Parameter(name="p1", datatype="str")])
    def s4(ctx: FunctionContext, i1: DataBlock):
        pass

    for snp in [s1, s2, s3, s4]:
        if snp in (s1, s2):
            assert snp.namespace == "module1"
        else:
            assert snp.namespace == DEFAULT_LOCAL_NAMESPACE
        assert snp.name == "s1"
        assert len(snp.params) == 1
        i = snp.get_interface()
        assert len(i.inputs) == 1
