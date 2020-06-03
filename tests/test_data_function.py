from __future__ import annotations

from typing import Callable

import pytest
from pandas import DataFrame

from basis.core.data_block import DataBlock, DataBlockMetadata
from basis.core.data_function import (
    DataFunctionInterface,
    DataFunctionLike,
    datafunction,
    ensure_datafunction,
    datafunction_chain,
)
from basis.core.data_function_interface import (
    DataFunctionAnnotation,
    FunctionGraphResolver,
)
from basis.core.function_node import (
    FunctionNode,
    FunctionNodeChain,
    function_node_factory,
)
from basis.core.sql.data_function import sql_datafunction
from basis.core.streams import DataBlockStream, InputBlocks
from basis.modules.core.dataset import DataSetAccumulator
from basis.utils.common import md5_hash
from basis.utils.registry import T, U
from tests.utils import (
    TestType1,
    TestType2,
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
            DataFunctionAnnotation(
                data_format_class="DataBlock",
                otype_like="Type",
                # is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataBlock[Type]",
            ),
        ),
        (
            "DataSet[Type]",
            DataFunctionAnnotation(
                data_format_class="DataSet",
                otype_like="Type",
                # is_iterable=False,
                is_generic=False,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataSet[Type]",
            ),
        ),
        (
            "DataBlock[T]",
            DataFunctionAnnotation(
                data_format_class="DataBlock",
                otype_like="T",
                # is_iterable=False,
                is_generic=True,
                is_optional=False,
                is_variadic=False,
                original_annotation="DataBlock[T]",
            ),
        ),
    ],
)
def test_typed_annotation(annotation: str, expected: DataFunctionAnnotation):
    tda = DataFunctionAnnotation.from_type_annotation(annotation)
    assert tda == expected


def df_notworking(_1: int, _2: str, input: DataBlock[TestType1]):
    # Bad args
    pass


def df4(input: DataBlock[T], dr2: DataBlock[U], dr3: DataBlock[U],) -> DataFrame[T]:
    pass


def df_self(input: DataBlock[T], this: DataBlock[T] = None) -> DataFrame[T]:
    pass


df_chain = datafunction_chain("df_chain", [df_t1_to_t2, df_generic])


@pytest.mark.parametrize(
    "function,expected",
    [
        (
            df_t1_sink,
            DataFunctionInterface(
                inputs=[
                    DataFunctionAnnotation(
                        data_format_class="DataBlock",
                        otype_like="TestType1",
                        name="input",
                        # is_iterable=False,
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
                    DataFunctionAnnotation(
                        data_format_class="DataBlock",
                        otype_like="TestType1",
                        name="input",
                        # is_iterable=False,
                        is_generic=False,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[TestType1]",
                    )
                ],
                output=DataFunctionAnnotation(
                    data_format_class="DataFrame",
                    otype_like="TestType2",
                    # is_iterable=False,
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
                    DataFunctionAnnotation(
                        data_format_class="DataBlock",
                        otype_like="T",
                        name="input",
                        # is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        original_annotation="DataBlock[T]",
                    )
                ],
                output=DataFunctionAnnotation(
                    data_format_class="DataFrame",
                    otype_like="T",
                    # is_iterable=False,
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
                    DataFunctionAnnotation(
                        data_format_class="DataBlock",
                        otype_like="T",
                        name="input",
                        # is_iterable=False,
                        is_generic=True,
                        is_optional=False,
                        is_variadic=False,
                        is_self_ref=False,
                        original_annotation="DataBlock[T]",
                    ),
                    DataFunctionAnnotation(
                        data_format_class="DataBlock",
                        otype_like="T",
                        name="this",
                        # is_iterable=False,
                        is_generic=True,
                        is_optional=True,
                        is_variadic=False,
                        is_self_ref=True,
                        original_annotation="DataBlock[T]",
                    ),
                ],
                output=DataFunctionAnnotation(
                    data_format_class="DataFrame",
                    otype_like="T",
                    # is_iterable=False,
                    is_generic=True,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[T]",
                ),
                requires_data_function_context=False,
            ),
        ),
        # (
        #     df_chain,
        #     DataFunctionInterface(
        #         inputs=[
        #             DataFunctionAnnotation(
        #                 data_format_class="DataBlock",
        #                 otype_like="TestType1",
        #                 name="input",
        #                 # is_iterable=False,
        #                 is_generic=False,
        #                 is_optional=False,
        #                 is_variadic=False,
        #                 original_annotation="DataBlock[TestType1]",
        #             )
        #         ],
        #         output=DataFunctionAnnotation(
        #             data_format_class="DataFrame",
        #             otype_like="T",
        #             # is_iterable=False,
        #             is_generic=True,
        #             is_optional=False,
        #             is_variadic=False,
        #             original_annotation="DataFrame[T]",
        #         ),
        #         requires_data_function_context=False,
        #     ),
        # ),
    ],
)
def test_data_function_interface(
    function: DataFunctionLike, expected: DataFunctionInterface
):
    env = make_test_env()
    if isinstance(function, Callable):
        val = DataFunctionInterface.from_datafunction_definition(function)
        assert val == expected
    node = function_node_factory(env, "_test", function)
    assert node._get_interface() == expected


# env = make_test_env()
# upstream = env.add_node("_test_df1", df_t1_source)
#
#
# @pytest.mark.parametrize(
#     "function,expected",
#     [
#         (
#             df_t1_sink,
#             DataFunctionInterface(
#                 inputs=[
#                     DataFunctionAnnotation(
#                         data_format_class="DataBlock",
#                         otype_like="TestType1",
#                         resolved_otype=TestType1,
#                         connected_stream=upstream,
#                         name="input",
#                         is_iterable=False,
#                         is_optional=False,
#                         original_annotation="DataBlock[TestType1]",
#                     )
#                 ],
#                 output=None,
#                 requires_data_function_context=True,
#                 is_connected=True,
#                 is_resolved=True,
#             ),
#         ),
#         (
#             df_generic,
#             DataFunctionInterface(
#                 inputs=[
#                     DataFunctionAnnotation(
#                         data_format_class="DataBlock",
#                         otype_like="T",
#                         resolved_otype=TestType1,
#                         connected_stream=upstream,
#                         name="input",
#                         is_generic=True,
#                         is_iterable=False,
#                         is_optional=False,
#                         original_annotation="DataBlock[T]",
#                     )
#                 ],
#                 output=DataFunctionAnnotation(
#                     data_format_class="DataFrame",
#                     otype_like="T",
#                     resolved_otype=TestType1,
#                     is_generic=True,
#                     is_iterable=False,
#                     is_optional=False,
#                     original_annotation="DataFrame[T]",
#                 ),
#                 requires_data_function_context=False,
#                 is_connected=True,
#                 is_resolved=True,
#             ),
#         ),
#     ],
# )
# def test_concrete_data_function_interface(
#     function: Callable, expected: DataFunctionInterface
# ):
#     dfi = DataFunctionInterface.from_datafunction_definition(function)
#     dfi.connect_upstream(upstream)
#     dfi.resolve_otypes(env)
#     assert dfi == expected


def test_upstream():
    env = make_test_env()
    n1 = env.add_node("node1", df_t1_source)
    n2 = env.add_node("node2", df_t1_to_t2, upstream={"input": "node1"})
    dfi = n2.get_interface()
    assert dfi is not None
    n3 = env.add_node("node3", df_chain, upstream="node1")
    dfi = n3.get_interface()
    assert dfi is not None


def test_stream_input():
    env = make_test_env()
    n1 = env.add_node("node1", df_t1_source)
    n2 = env.add_node("node2", df_t1_source)
    n3 = env.add_node("node3", df_chain, upstream="node1")
    ds1 = env.add_node(
        "ds1", DataSetAccumulator("type1"), upstream=DataBlockStream(otype="TestType1")
    )
    dfi = ds1.get_interface()


def test_python_data_function():
    df = datafunction(df_t1_sink)
    assert (
        df.key == df_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "key1"
    df = datafunction(df_t1_sink, key=k)
    assert df.key == k

    dfi = df.get_interface()
    assert dfi is not None


def test_sql_data_function():
    env = make_test_env()
    sql = "select:T 1 from t:T"
    k = "k1"
    df = sql_datafunction(k, sql)
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
    df = sql_datafunction("s1", sql)
    dfi = df.get_interface()
    assert dfi is not None

    assert len(dfi.inputs) == 2
    assert dfi.inputs[0].otype_like == "U"
    assert dfi.inputs[0].name == "t1"
    assert not dfi.inputs[0].is_optional
    assert dfi.inputs[1].otype_like == "T"
    assert dfi.inputs[1].name == "t2"
    assert dfi.inputs[1].is_optional


def test_function_node_no_inputs():
    env = make_test_env()
    df = datafunction(df_t1_source)
    node1 = FunctionNode(env, "node1", df)
    assert {node1: node1}[node1] is node1  # Test hash
    dfi = node1.get_interface()
    assert dfi.inputs == []
    assert dfi.output is not None
    assert node1.get_inputs() == {}
    assert node1.get_output_node() is node1
    assert not node1.is_composite()


def test_function_node_inputs():
    env = make_test_env()
    df = datafunction(df_t1_source)
    node = FunctionNode(env, "node", df)
    df = datafunction(df_t1_sink)
    with pytest.raises(Exception):
        # Bad input
        FunctionNode(env, "node_fail", df, input="Turkey")  # type: ignore
    node1 = FunctionNode(env, "node1", df, input=node)
    dfi = node1.get_interface()
    dfi = node1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is None
    assert list(node1.get_inputs().keys()) == ["input"]
    assert node1.get_input("input").get_upstream(env)[0] is node


def test_function_node_chain():
    env = make_test_env()
    df = datafunction(df_t1_source)
    node = FunctionNode(env, "node", df)
    node1 = FunctionNodeChain(env, "node1", df_chain, upstream=node)
    dfi = node1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is not None
    assert list(node1.get_inputs().keys()) == ["input"]
    assert node1.get_input("input").get_upstream(env)[0] is node
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


def test_graph_resolution():
    env = make_test_env()
    n1 = env.add_node("node1", df_t1_source)
    n2 = env.add_node("node2", df_t1_source)
    n3 = env.add_node("node3", df_chain, upstream="node1")
    n4 = env.add_node("node4", df_t1_to_t2, upstream="node2")
    n5 = env.add_node("node5", df_generic, upstream="node4")
    n6 = env.add_node("node6", df_self, upstream="node4")
    fgr = FunctionGraphResolver(env)
    # Resolve types
    assert fgr.resolve_output_type(n4) is TestType2
    assert fgr.resolve_output_type(n5) is TestType2
    assert fgr.resolve_output_type(n6) is TestType2
    fgr.resolve_output_types()
    last = n3.get_nodes()[-1]
    assert fgr._resolved_output_types[last] is TestType2
    # Resolve deps
    assert fgr._resolve_node_dependencies(n4)[0].parent_nodes == [n2]
    assert fgr._resolve_node_dependencies(n5)[0].parent_nodes == [n4]
    fgr.resolve_dependencies()
