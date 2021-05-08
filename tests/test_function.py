from __future__ import annotations
from snapflow.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
    DataFunctionOutputCfg,
    InputType,
)

from typing import Any, Callable

import pytest
from pandas import DataFrame
from snapflow.core.data_block import DataBlock, DataBlockMetadata
from snapflow.core.execution import DataFunctionContext
from snapflow.core.function import DataFunctionLike, datafunction
from snapflow.core.function_interface import (
    DEFAULT_OUTPUT,
    DEFAULT_OUTPUTS,
    ParsedAnnotation,
    parse_input_annotation,
)
from snapflow.core.function_interface_manager import get_schema_translation
from snapflow.core.graph import Graph, graph

from snapflow.core.module import DEFAULT_LOCAL_NAMESPACE
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


@pytest.mark.parametrize(
    "annotation,expected",
    [
        (
            "DataBlock[Type]",
            ParsedAnnotation(
                input_type=InputType("DataBlock"),
                schema="Type",
                optional=False,
                original_annotation="DataBlock[Type]",
            ),
        ),
        (
            "Optional[DataBlock[Type]]",
            ParsedAnnotation(
                input_type=InputType("DataBlock"),
                schema="Type",
                optional=True,
                original_annotation="Optional[DataBlock[Type]]",
            ),
        ),
        (
            "SelfReference[Type]",
            ParsedAnnotation(
                input_type=InputType("SelfReference"),
                schema="Type",
                optional=False,
                original_annotation="SelfReference[Type]",
            ),
        ),
        (
            "Reference[T]",
            ParsedAnnotation(
                input_type=InputType("Reference"),
                schema="T",
                optional=False,
                original_annotation="Reference[T]",
            ),
        ),
        (
            "Stream[Type]",
            ParsedAnnotation(
                input_type=InputType("Stream"),
                schema="Type",
                optional=False,
                original_annotation="Stream[Type]",
            ),
        ),
    ],
)
def test_typed_annotation(annotation: str, expected: ParsedAnnotation):
    tda = parse_input_annotation(annotation)
    assert tda == expected


def function_notworking(_1: int, _2: str, input: DataBlock[TestSchema1]):
    # Bad args
    pass


def df4(input: DataBlock[T], dr2: DataBlock[U], dr3: DataBlock[U],) -> DataFrame[T]:
    pass


@pytest.mark.parametrize(
    "function_like,expected",
    [
        (
            function_t1_sink,
            DataFunctionInterfaceCfg(
                inputs={
                    "input": DataFunctionInputCfg(
                        name="input",
                        input_type=InputType("DataBlock"),
                        schema_key="TestSchema1",
                        required=True,
                    ),
                },
                outputs={DEFAULT_OUTPUT_NAME: DEFAULT_OUTPUT},
                parameters={},
                uses_context=True,
            ),
        ),
        (
            function_t1_to_t2,
            DataFunctionInterfaceCfg(
                inputs={
                    "input": DataFunctionInputCfg(
                        name="input",
                        input_type=InputType("DataBlock"),
                        schema_key="TestSchema1",
                        required=True,
                    ),
                },
                outputs={
                    DEFAULT_OUTPUT_NAME: DataFunctionOutputCfg(
                        data_format="DataFrame", schema_key="TestSchema2"
                    )
                },
                parameters={},
                uses_context=True,
            ),
        ),
        (
            function_generic,
            DataFunctionInterfaceCfg(
                inputs={
                    "input": DataFunctionInputCfg(
                        name="input",
                        input_type=InputType("DataBlock"),
                        schema_key="T",
                        required=True,
                    ),
                },
                outputs={
                    DEFAULT_OUTPUT_NAME: DataFunctionOutputCfg(
                        data_format="DataFrame", schema_key="T"
                    )
                },
                parameters={},
                uses_context=False,
            ),
        ),
        (
            function_self,
            DataFunctionInterfaceCfg(
                inputs={
                    "input": DataFunctionInputCfg(
                        name="input",
                        input_type=InputType("DataBlock"),
                        schema_key="T",
                        required=True,
                    ),
                    "previous": DataFunctionInputCfg(
                        name="previous",
                        input_type=InputType("SelfReference"),
                        schema_key="T",
                        required=False,
                    ),
                },
                outputs={
                    DEFAULT_OUTPUT_NAME: DataFunctionOutputCfg(
                        data_format="DataFrame", schema_key="T"
                    )
                },
                parameters={},
                uses_context=False,
            ),
        ),
    ],
)
def test_function_interface(
    function_like: DataFunctionLike, expected: DataFunctionInterfaceCfg
):
    p = datafunction(function_like)
    val = p.get_interface()
    assert set(val.inputs) == set(expected.inputs)
    assert val.outputs == expected.outputs
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
        exe = ExecutableCfg(node=n1, function=n1.function, execution_context=ec)
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
        nominal_schema_key="_test.TestSchema1", realized_schema_key="_test.TestSchema1",
    )
    # stream = block_as_stream(block, ec, pi.inputs[0].schema(env), translation)
    # bi = im.get_bound_stream_interface({"input": stream})
    # assert len(bi.inputs) == 1
    # input: StreamInput = bi.inputs[0]
    with env.md_api.begin():
        schema_translation = get_schema_translation(
            env,
            block.realized_schema(env),
            target_schema=env.get_schema(
                pi.get_single_non_recursive_input().schema_key
            ),
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
        nominal_schema_key="_test.TestSchema1", realized_schema_key="_test.TestSchema1",
    )
    with env.md_api.begin():
        schema_translation = get_schema_translation(
            env,
            block.realized_schema(env),
            target_schema=env.get_schema(
                pi.get_single_non_recursive_input().schema_key
            ),
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
    p = datafunction(function_t1_sink)
    assert (
        p.name == function_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "name1"
    p = datafunction(function_t1_sink, name=k)
    assert p.name == k
    assert p.key == f"{DEFAULT_LOCAL_NAMESPACE}.{k}"

    pi = p.get_interface()
    assert pi is not None


@datafunction("k1")
def df1():
    pass


@datafunction("k1", required_storage_classes=["database"])
def df2():
    pass


def test_node_no_inputs():
    env = make_test_env()
    g = Graph(env)
    df = datafunction(function_t1_source)
    node1 = g.create_node(key="node1", function=df)
    assert {node1: node1}[node1] is node1  # Test hash
    pi = node1.get_interface()
    assert pi.inputs == {}
    assert pi.outputs != {}
    assert node1.declared_inputs == {}


def test_node_inputs():
    env = make_test_env()
    g = Graph(env)
    df = datafunction(function_t1_source)
    node = g.create_node(key="node", function=df)
    df = datafunction(function_t1_sink)
    node1 = g.create_node(key="node1", function=df, input=node)
    pi = node1.get_interface()
    assert len(pi.inputs) == 1
    assert pi.outputs == DEFAULT_OUTPUTS
    assert list(node1.declared_inputs.keys()) == ["input"]


def test_node_stream_inputs():
    pi = datafunction(function_stream).get_interface()
    assert len(pi.inputs) == 1
    assert pi.get_single_non_recursive_input().input_type == InputType("Stream")


def test_node_params():
    env = make_test_env()
    g = Graph(env)
    param_vals = []

    def function_ctx(ctx: DataFunctionContext, test: str):
        param_vals.append(test)

    n = g.create_node(key="ctx", function=function_ctx, params={"test": 1})
    env.run_node(n, g)
    assert param_vals == [1]


def test_any_schema_interface():
    env = make_test_env()
    env.add_module(core)

    def function_any(input: DataBlock) -> DataFrame:
        pass

    df = datafunction(function_any)
    pi = df.get_interface()
    assert pi.get_single_non_recursive_input().schema_key == "Any"
    assert pi.get_default_output().schema_key == "Any"


def test_api():
    @datafunction(namespace="module1")
    def s1(ctx, i1, p1: str):
        pass

    @datafunction("s1", namespace="module1")
    def s2(ctx: DataFunctionContext, i1: DataBlock, p1: str = "default val"):
        pass

    @datafunction("s1")
    def s3(ctx, i1: DataBlock[TestSchema1], p1: str):
        pass

    # @datafunction(name="s1", params=[Parameter(name="p1", datatype="str")])
    # def s4(ctx: DataFunctionContext, i1: DataBlock) -> Any:
    #     pass

    for snp in [s1, s2, s3]:  # , s4]:
        if snp in (s1, s2):
            assert snp.namespace == "module1"
        else:
            assert snp.namespace == DEFAULT_LOCAL_NAMESPACE
        assert snp.name == "s1"
        assert len(snp.params) == 1
        i = snp.get_interface()
        assert len(i.inputs) == 1
