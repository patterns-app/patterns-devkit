from __future__ import annotations

from typing import Callable

import pytest
from pandas import DataFrame

from basis.core.data_block import DataBlock, DataBlockMetadata
from basis.core.data_function import (
    ConfiguredDataFunctionChain,
    DataFunctionChain,
    DataFunctionInterface,
    DataFunctionLike,
    FunctionNode,
    PythonDataFunction,
    ResolvedDataFunctionInterface,
    ResolvedTypedDataAnnotation,
    TypedDataAnnotation,
    configured_data_function_factory,
)
from basis.core.graph import FunctionGraph
from basis.core.sql.data_function import SqlDataFunction
from basis.core.streams import DataBlockStream, InputResources
from basis.utils.common import md5_hash
from basis.utils.registry import T, U
from tests.utils import (
    TestType1,
    df_generic,
    df_t1_sink,
    df_t1_source,
    df_t1_to_t2,
    make_test_env,
    make_test_execution_context,
)


@pytest.mark.parametrize(
    "annotation,expected",
    [
        (
            "DataBlock[Type]",
            TypedDataAnnotation(
                data_block_class="DataBlock",
                otype_like="Type",
                is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataBlock[Type]",
            ),
        ),
        (
            "DataSet[Type]",
            TypedDataAnnotation(
                data_block_class="DataSet",
                otype_like="Type",
                is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataSet[Type]",
            ),
        ),
        (
            "DataBlock[T]",
            TypedDataAnnotation(
                data_block_class="DataBlock",
                otype_like="T",
                is_iterable=False,
                is_generic=True,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataBlock[T]",
            ),
        ),
    ],
)
def test_typed_annotation(annotation: str, expected: TypedDataAnnotation):
    tda = TypedDataAnnotation.from_type_annotation(annotation)
    assert tda == expected


def df_notworking(_1: int, _2: str, block: DataBlock[TestType1]):
    # TODO: how should DFI handle non-DR args?
    pass


def df4(block: DataBlock[T], dr2: DataBlock[U], dr3: DataBlock[U],) -> DataFrame[T]:
    pass


def df_self(block: DataBlock[T], this: DataBlock[T] = None) -> DataFrame[T]:
    pass


df_chain = DataFunctionChain("df_chain", [df_t1_to_t2, df_generic])


@pytest.mark.parametrize(
    "function,expected",
    [
        (
            df_t1_sink,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_block_class="DataBlock",
                        otype_like="TestType1",
                        name="block",
                        is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[TestType1]",
                    )
                ],
                output=None,
                requires_data_function_context=True,
            ),
        ),
        (
            df_t1_to_t2,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_block_class="DataBlock",
                        otype_like="TestType1",
                        name="block",
                        is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[TestType1]",
                    )
                ],
                output=TypedDataAnnotation(
                    data_block_class="DataFrame",
                    otype_like="TestType2",
                    is_iterable=False,
                    is_generic=False,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[TestType2]",
                ),
                requires_data_function_context=False,
            ),
        ),
        (
            df_generic,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_block_class="DataBlock",
                        otype_like="T",
                        name="block",
                        is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[T]",
                    )
                ],
                output=TypedDataAnnotation(
                    data_block_class="DataFrame",
                    otype_like="T",
                    is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_data_function_context=False,
            ),
        ),
        (
            df_self,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_block_class="DataBlock",
                        otype_like="T",
                        name="block",
                        is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        is_self_ref=False,
                        original_annotation="DataBlock[T]",
                    ),
                    TypedDataAnnotation(
                        data_block_class="DataBlock",
                        otype_like="T",
                        name="this",
                        is_iterable=False,
                        is_generic=True,
                        is_optional=True,
                        is_variadic=False,
                        is_self_ref=True,
                        original_annotation="DataBlock[T]",
                    ),
                ],
                output=TypedDataAnnotation(
                    data_block_class="DataFrame",
                    otype_like="T",
                    is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_data_function_context=False,
            ),
        ),
        (
            df_chain,
            DataFunctionInterface(
                inputs=[
                    TypedDataAnnotation(
                        data_block_class="DataBlock",
                        otype_like="TestType1",
                        name="block",
                        is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[TestType1]",
                    )
                ],
                output=TypedDataAnnotation(
                    data_block_class="DataFrame",
                    otype_like="T",
                    is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_data_function_context=False,
            ),
        ),
    ],
)
def test_data_function_interface(
    function: DataFunctionLike, expected: DataFunctionInterface
):
    if isinstance(function, Callable):
        val = DataFunctionInterface.from_datafunction_definition(function)
        assert val == expected
    node = configured_data_function_factory(env, "_test", function)
    assert node.get_interface() == expected


env = (
    make_test_env()
)  # TODO: what is this doing in globals?? Make anew for each test pls thx


@pytest.mark.parametrize(
    "function,expected",
    [
        (
            df_t1_sink,
            ResolvedDataFunctionInterface(
                inputs=[
                    ResolvedTypedDataAnnotation(
                        data_block_class="DataBlock",
                        otype=TestType1,
                        name="block",
                        is_iterable=False,
                        is_optional=False,
                        original_annotation="DataBlock[TestType1]",
                    )
                ],
                output=None,
                requires_data_function_context=True,
            ),
        ),
        (
            df_generic,
            ResolvedDataFunctionInterface(
                inputs=[
                    ResolvedTypedDataAnnotation(
                        data_block_class="DataBlock",
                        otype=TestType1,
                        name="block",
                        is_iterable=False,
                        is_optional=False,
                        original_annotation="DataBlock[T]",
                    )
                ],
                output=ResolvedTypedDataAnnotation(
                    data_block_class="DataFrame",
                    otype=TestType1,
                    is_iterable=False,
                    is_optional=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_data_function_context=False,
            ),
        ),
    ],
)
def test_concrete_data_function_interface(
    function: Callable, expected: ResolvedDataFunctionInterface
):
    input_data_blocks: InputResources = {
        "block": DataBlockMetadata(otype_uri="_test.TestType1"),
    }
    dfi = DataFunctionInterface.from_datafunction_definition(function)
    nodei = ResolvedDataFunctionInterface.from_data_function_interface(
        env, input_data_blocks, dfi
    )
    assert nodei == expected


def test_python_data_function():
    df = PythonDataFunction(df_t1_sink)
    assert (
        df.key == df_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "key1"
    df = PythonDataFunction(df_t1_sink, key=k)
    assert df.key == k

    dfi = df.get_interface()
    assert dfi is not None


def test_sql_data_function():
    sql = "select:T 1 from t:T"
    df = SqlDataFunction(sql)
    assert df.key == md5_hash(sql)

    k = "key1"
    df = SqlDataFunction(sql, key=k)
    assert df.key == k

    dfi = df.get_interface()
    assert dfi is not None

    assert len(dfi.inputs) == 1
    assert dfi.inputs[0].otype_like == "T"
    assert dfi.inputs[0].name == "t"
    assert dfi.output is not None
    assert dfi.output.otype_like == "T"


def test_sql_data_function2():
    sql = "select:T 1 from from t1:U join t2:Optional[T]"
    df = SqlDataFunction(sql)
    dfi = df.get_interface()
    assert dfi is not None

    assert len(dfi.inputs) == 2
    assert dfi.inputs[0].otype_like == "U"
    assert dfi.inputs[0].name == "t1"
    assert not dfi.inputs[0].is_optional
    assert dfi.inputs[1].otype_like == "T"
    assert dfi.inputs[1].name == "t2"
    assert dfi.inputs[1].is_optional


def test_configured_data_function_no_inputs():
    df = PythonDataFunction(df_t1_source)
    node1 = FunctionNode(env, "node1", df)
    assert {node1: node1}[node1] is node1  # Test hash
    dfi = node1.get_interface()
    assert dfi.inputs == []
    assert dfi.output is not None
    assert node1.get_inputs() == {}
    assert node1.get_output_node() is node1
    assert not node1.is_graph()


def test_configured_data_function_inputs():
    df = PythonDataFunction(df_t1_source)
    node = FunctionNode(env, "node", df)
    df = PythonDataFunction(df_t1_sink)
    with pytest.raises(Exception):
        # Bad input
        FunctionNode(env, "node_fail", df, input="Turkey")  # type: ignore
    node1 = FunctionNode(env, "node1", df, input=node)
    dfi = node1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is None
    assert node1.get_inputs() == {"input": node}
    assert node1.get_input("input") is node
    assert node1.get_data_stream_input("input") is node


def test_configured_data_function_chain():
    df = PythonDataFunction(df_t1_source)
    node = FunctionNode(env, "node", df)
    node1 = ConfiguredDataFunctionChain(env, "node1", df_chain, input=node)
    dfi = node1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is not None
    assert node1.get_inputs() == {"input": node}
    assert node1.get_input("input") is node
    assert node1.get_data_stream_input("input") is node
    # Output NODE
    output_node = node1.get_output_node()
    assert output_node is not node1
    assert node1.key in output_node.key
    out_dfi = output_node.get_interface()
    assert len(out_dfi.inputs) == 1
    assert out_dfi.output is not None
    # Children
    children = node1.build_nodes()
    assert len(children) == 2
    assert len(node1.get_nodes()) == 2
