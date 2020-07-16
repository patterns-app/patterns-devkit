from __future__ import annotations

from typing import Callable, Dict

import pytest
from pandas import DataFrame

from basis.core.component import ComponentType
from basis.core.data_block import DataBlock
from basis.core.data_function import (
    DataFunction,
    DataFunctionInterface,
    DataFunctionLike,
    data_function,
    data_function_chain,
)
from basis.core.data_function_interface import (
    DataFunctionAnnotation,
    FunctionGraphResolver,
)
from basis.core.node import FunctionNodeChain, Node, function_node_factory
from basis.core.runnable import DataFunctionContext
from basis.core.runtime import RuntimeClass
from basis.core.sql.data_function import sql_data_function
from basis.core.streams import DataBlockStream
from basis.modules import core
from basis.modules.core.functions.accumulate_as_dataset import accumulate_as_dataset
from basis.utils.typing import T, U
from tests.utils import (
    TestType1,
    TestType2,
    df_generic,
    df_t1_sink,
    df_t1_source,
    df_t1_to_t2,
    make_test_env,
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


df_chain = data_function_chain("df_chain", [df_t1_to_t2, df_generic])


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
        val = DataFunctionInterface.from_data_function_definition(function)
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
#     dfi = DataFunctionInterface.from_data_function_definition(function)
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
    env.add_module(core)
    n1 = env.add_node("node1", df_t1_source)
    n2 = env.add_node("node2", df_t1_source)
    n3 = env.add_node("node3", df_chain, upstream="node1")
    ds1 = env.add_node(
        "ds1",
        accumulate_as_dataset,
        config=dict(dataset_name="type1"),
        upstream=DataBlockStream(otype="TestType1"),
    )
    dfi = ds1.get_interface()


def test_python_data_function():
    df = data_function(df_t1_sink)
    assert (
        df.name == df_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "name1"
    df = data_function(df_t1_sink, name=k)
    assert df.name == k

    dfi = df.get_interface()
    assert dfi is not None


def test_sql_data_function():
    env = make_test_env()
    sql = "select:T 1 from t:T"
    k = "k1"
    df = sql_data_function(k, sql)
    assert df.name == k

    dfi = df.get_interface()
    assert dfi is not None

    assert len(dfi.inputs) == 1
    assert dfi.inputs[0].otype_like == "T"
    assert dfi.inputs[0].name == "t"
    assert dfi.output is not None
    assert dfi.output.otype_like == "T"


def test_sql_data_function2():
    sql = "select:T 1 from from t1:U join t2:Optional[T]"
    df = sql_data_function("s1", sql)
    dfi = df.get_interface()
    assert dfi is not None

    assert len(dfi.inputs) == 2
    assert dfi.inputs[0].otype_like == "U"
    assert dfi.inputs[0].name == "t1"
    assert not dfi.inputs[0].is_optional
    assert dfi.inputs[1].otype_like == "T"
    assert dfi.inputs[1].name == "t2"
    assert dfi.inputs[1].is_optional


def test_sql_data_function_no_types():
    sql = "select 1 from from t1 join t2 on t1.a = t2.b left join t3 on"
    df = sql_data_function("s1", sql)
    dfi = df.get_interface()
    assert dfi is not None

    assert len(dfi.inputs) == 3
    assert dfi.inputs[0].otype_like == "Any"
    assert dfi.inputs[0].name == "t1"
    assert dfi.inputs[1].otype_like == "Any"
    assert dfi.inputs[1].name == "t2"
    assert dfi.inputs[2].otype_like == "Any"
    assert dfi.inputs[2].name == "t3"

    sql = """select 1 from
    from -- comment inbetween
    t1
    join t2 on t1.a = t2.b"""
    df = sql_data_function("s1", sql)
    dfi = df.get_interface()
    assert dfi is not None
    assert len(dfi.inputs) == 2

    sql = """select 1, 'not a commment -- nope'
    from -- comment inbetween
    t1, t2 on t1.a = t2.b"""
    df = sql_data_function("s1", sql)
    dfi = df.get_interface()
    assert dfi is not None
    assert len(dfi.inputs) == 2


@data_function("k1", compatible_runtimes="python")
def df1():
    pass


@data_function("k1", compatible_runtimes="mysql")
def df2():
    pass


def test_data_function_set():
    dfs = DataFunction(
        name="k1",
        component_type=ComponentType.DataFunction,
        version=None,
        module_name=None,
    )
    dfs.add_definition(df1)
    assert dfs.runtime_data_functions[RuntimeClass.PYTHON] is df1
    dfs.add_definition(df2)
    assert dfs.runtime_data_functions[RuntimeClass.DATABASE] is df2
    dfs1 = DataFunction(
        name="k1",
        component_type=ComponentType.DataFunction,
        version=None,
        module_name=None,
    )
    dfs1.add_definition(df1)
    dfs2 = DataFunction(
        name="k1",
        component_type=ComponentType.DataFunction,
        version=None,
        module_name=None,
    )
    dfs2.add_definition(df2)
    dfs1.merge(dfs2)
    assert dfs1.runtime_data_functions[RuntimeClass.PYTHON] is df1
    assert dfs1.runtime_data_functions[RuntimeClass.DATABASE] is df2


# def test_data_function_registry():
#     r = DataFunctionRegistry()
#     dfs = DataFunction(name="k1", module_name=DEFAULT_MODULE_NAME, version=None)
#     dfs.add_definition(df1)
#     r.process_and_register_all([df_t1_sink, df_chain, dfs, df2])
#     assert r.get("k1") is dfs
#     assert r.get("k1").runtime_data_functions[RuntimeClass.PYTHON] is df1
#     assert r.get("k1").runtime_data_functions[RuntimeClass.DATABASE] is df2


def test_function_node_no_inputs():
    env = make_test_env()
    df = data_function(df_t1_source)
    node1 = Node(env, "node1", df)
    assert {node1: node1}[node1] is node1  # Test hash
    dfi = node1.get_interface()
    assert dfi.inputs == []
    assert dfi.output is not None
    assert node1.get_inputs() == {}
    assert node1.get_output_node() is node1
    assert not node1.is_composite()


def test_function_node_inputs():
    env = make_test_env()
    df = data_function(df_t1_source)
    node = Node(env, "node", df)
    df = data_function(df_t1_sink)
    with pytest.raises(Exception):
        # Bad input
        Node(env, "node_fail", df, input="Turname")  # type: ignore
    node1 = Node(env, "node1", df, upstream=node)
    dfi = node1.get_interface()
    dfi = node1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is None
    assert list(node1.get_inputs().keys()) == ["input"]
    assert node1.get_input("input").get_upstream(env)[0] is node


def test_function_node_config():
    env = make_test_env()
    config_vals = []

    def df_ctx(ctx: DataFunctionContext):
        config_vals.append(ctx.config("test"))

    ctx = env.add_node("ctx", df_ctx, config={"test": 1, "extra_arg": 2})
    with env.execution() as exe:
        exe.run(ctx)
    assert config_vals == [1]


def test_function_node_chain():
    env = make_test_env()
    df = data_function(df_t1_source)
    node = Node(env, "node", df)
    node1 = FunctionNodeChain(env, "node1", df_chain, upstream=node)
    dfi = node1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is not None
    assert list(node1.get_inputs().keys()) == ["input"]
    assert node1.get_input("input").get_upstream(env)[0] is node
    # Output NODE
    output_node = node1.get_output_node()
    assert output_node is not node1
    assert node1.name in output_node.name
    out_dfi = output_node.get_interface()
    assert len(out_dfi.inputs) == 1
    assert out_dfi.output is not None
    # Children
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
    # Otype resolution
    n7 = env.add_node("node7", df_self, upstream=DataBlockStream(otype="TestType2"))
    n8 = env.add_node("node8", df_self, upstream=n7)
    fgr = env.get_function_graph_resolver()
    fgr.resolve()
    parent_keys = set(
        p.name for p in fgr.get_resolved_interface(n7).inputs[0].parent_nodes
    )
    assert parent_keys == {
        "node3__df_t1_to_t2",
        "node3__df_generic",
        "node4",
        "node5",
        "node6",
    }


def test_any_otype_interface():
    env = make_test_env()
    env.add_module(core)

    def df_any(input: DataBlock) -> DataFrame:
        pass

    df = data_function(df_any)
    dfi = df.get_interface()
    assert dfi.inputs[0].otype_like == "Any"
    assert dfi.output.otype_like == "Any"
