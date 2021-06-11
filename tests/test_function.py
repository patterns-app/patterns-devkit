from __future__ import annotations

from typing import Any, Callable

import pytest
from pandas import DataFrame
from snapflow import DataFunctionContext
from snapflow.core.component import global_library
from snapflow.core.data_block import DataBlock
from snapflow.core.declarative.base import update
from snapflow.core.declarative.execution import ExecutableCfg
from snapflow.core.declarative.function import (
    DEFAULT_OUTPUT_NAME,
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
    DataFunctionOutputCfg,
    InputType,
)
from snapflow.core.declarative.graph import GraphCfg
from snapflow.core.declarative.interface import BoundInterfaceCfg
from snapflow.core.execution.run import prepare_executable
from snapflow.core.function import DataFunctionLike, datafunction
from snapflow.core.function_interface import (
    DEFAULT_OUTPUT,
    DEFAULT_OUTPUTS,
    ParsedAnnotation,
    parse_input_annotation,
)
from snapflow.core.function_interface_manager import (
    get_bound_interface,
    get_schema_translation,
)
from snapflow.core.module import DEFAULT_LOCAL_NAMESPACE
from snapflow.core.persistence.data_block import DataBlockMetadata
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
    # node = DeclaredNode(key="_test", function="function" inputs={"input": "mock"}).instantiate(
    #     env
    # )
    # assert node.get_interface() == expected


def test_generic_schema_resolution():
    env = make_test_env()
    # make_test_run_context(env)
    n0 = GraphCfg(key="n0", function="function_t1_source").resolve()
    n1 = GraphCfg(key="node1", function="function_generic", input="n0").resolve()
    g = GraphCfg(nodes=[n0, n1])
    g = g.resolve(env.library)
    # pi = n1.get_interface()
    with env.md_api.begin():
        block = DataBlockMetadata(
            nominal_schema_key="_test.TestSchema1",
            realized_schema_key="_test.TestSchema2",
        )
        env.md_api.add(block)
        env.md_api.flush([block])
        inputs = n1.get_node_inputs(g)
        stream = [block.to_pydantic_with_stored()]
        bound_inputs = {"input": inputs["input"].as_bound_input(bound_stream=stream)}
        bi = BoundInterfaceCfg(inputs=bound_inputs, interface=n1.get_interface())
        assert len(bi.inputs) == 1
        assert bi.resolve_nominal_output_schema() == TestSchema1.key


def test_declared_schema_translation():
    env = make_test_env()

    translation = {"f1": "mapped_f1"}
    n1 = GraphCfg(
        key="node1",
        function="function_t1_to_t2",
        input="n0",
        schema_translation=translation,
    )
    pi = n1.resolve(global_library).get_interface()
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
            env.get_schema(block.realized_schema_key),
            target_schema=env.get_schema(
                pi.get_single_non_reference_input().schema_key
            ),
            declared_schema_translation=translation,
        )
        assert schema_translation.as_dict() == translation


def test_natural_schema_translation():
    env = make_test_env()

    translation = {"f1": "mapped_f1"}
    n1 = GraphCfg(
        key="node1",
        function="function_t1_to_t2",
        input="n0",
        schema_translation=translation,
    ).resolve(global_library)
    pi = n1.get_interface()
    # im = NodeInterfaceManager(ctx=ec, node=n1)
    block = DataBlockMetadata(
        nominal_schema_key="_test.TestSchema1",
        realized_schema_key="_test.TestSchema1",
    )
    with env.md_api.begin():
        schema_translation = get_schema_translation(
            env.get_schema(block.realized_schema_key),
            target_schema=env.get_schema(
                pi.get_single_non_reference_input().schema_key
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
    env = make_test_env()
    ec = make_test_run_context(env)
    n1 = GraphCfg(key="n1", function="function_t1_source").resolve()
    n2 = GraphCfg(function="function_t1_to_t2", inputs={"input": "n1"}).resolve()
    g = GraphCfg(nodes=[n1, n2])
    pi = n2.get_interface()
    assert pi is not None

    # ec.graph = g.instantiate(env)
    with env.md_api.begin():
        bi = get_bound_interface(env, ec, n1, g)
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
    node1 = GraphCfg(key="node1", function="function_t1_source").resolve()
    # assert {node1: node1}[node1] is node1  # Test hash
    pi = node1.get_interface()
    assert pi.inputs == {}
    assert pi.outputs != {}
    assert node1.inputs == {}


def test_node_inputs():
    node = GraphCfg(key="node", function="function_t1_source")
    node1 = GraphCfg(key="node1", function="function_t1_sink", input=node.key)
    pi = node1.resolve().get_interface()
    assert len(pi.inputs) == 1
    assert pi.outputs == DEFAULT_OUTPUTS
    assert list(node1.get_inputs().keys()) == ["stdin"]


def test_node_stream_inputs():
    pi = datafunction(function_stream).get_interface()
    assert len(pi.inputs) == 1
    assert pi.get_single_non_reference_input().input_type == InputType("Stream")


def test_node_params():
    env = make_test_env()
    param_vals = []

    @datafunction
    def function_ctx(ctx: DataFunctionContext, test: str):
        param_vals.append(test)

    env.add_function(function_ctx)

    n = GraphCfg(key="ctx", function="function_ctx", params={"test": 1})
    g = GraphCfg(nodes=[n]).resolve()
    env.run_node(n, g)
    assert param_vals == [1]


def test_any_schema_interface():
    env = make_test_env()
    env.add_module(core)

    @datafunction
    def function_any(input: DataBlock) -> DataFrame:
        pass

    pi = function_any.get_interface()
    assert pi.get_single_non_reference_input().schema_key == "Any"
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
