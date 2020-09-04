from __future__ import annotations

from typing import Callable

import pytest
from pandas import DataFrame

from dags.core.data_block import DataBlock
from dags.core.node import Node
from dags.core.pipe import Pipe, PipeInterface, PipeLike, pipe
from dags.core.pipe_interface import PipeAnnotation
from dags.core.runnable import PipeContext
from dags.core.runtime import RuntimeClass
from dags.core.sql.pipe import sql_pipe
from dags.modules import core
from dags.utils.typing import T, U
from tests.utils import (
    TestType1,
    df_chain_t1_to_t2,
    df_generic,
    df_self,
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
            PipeAnnotation(
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
            PipeAnnotation(
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
            PipeAnnotation(
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
def test_typed_annotation(annotation: str, expected: PipeAnnotation):
    tda = PipeAnnotation.from_type_annotation(annotation)
    assert tda == expected


def df_notworking(_1: int, _2: str, input: DataBlock[TestType1]):
    # Bad args
    pass


def df4(input: DataBlock[T], dr2: DataBlock[U], dr3: DataBlock[U],) -> DataFrame[T]:
    pass


@pytest.mark.parametrize(
    "pipe,expected",
    [
        (
            df_t1_sink,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
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
                requires_pipe_context=True,
            ),
        ),
        (
            df_t1_to_t2,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
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
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    otype_like="TestType2",
                    # is_iterable=False,
                    is_generic=False,
                    is_optional=False,
                    is_variadic=False,
                    original_annotation="DataFrame[TestType2]",
                ),
                requires_pipe_context=False,
            ),
        ),
        (
            df_generic,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
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
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    otype_like="T",
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
            df_self,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
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
                    PipeAnnotation(
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
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    otype_like="T",
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
            df_chain_t1_to_t2,
            PipeInterface(
                inputs=[
                    PipeAnnotation(
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
                output=PipeAnnotation(
                    data_format_class="DataFrame",
                    otype_like="T",
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
        val = pipe.get_interface(env)
    elif isinstance(pipe, Callable):
        val = PipeInterface.from_pipe_definition(pipe)
    assert val == expected
    node = Node(env, "_test", pipe, inputs="mock")
    assert node._get_interface() == expected


# env = make_test_env()
# upstream = env.add_node("_test_df1", df_t1_source)
#
#
# @pytest.mark.parametrize(
#     "pipe,expected",
#     [
#         (
#             df_t1_sink,
#             PipeInterface(
#                 inputs=[
#                     PipeAnnotation(
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
#                 requires_pipe_context=True,
#                 is_connected=True,
#                 is_resolved=True,
#             ),
#         ),
#         (
#             df_generic,
#             PipeInterface(
#                 inputs=[
#                     PipeAnnotation(
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
#                 output=PipeAnnotation(
#                     data_format_class="DataFrame",
#                     otype_like="T",
#                     resolved_otype=TestType1,
#                     is_generic=True,
#                     is_iterable=False,
#                     is_optional=False,
#                     original_annotation="DataFrame[T]",
#                 ),
#                 requires_pipe_context=False,
#                 is_connected=True,
#                 is_resolved=True,
#             ),
#         ),
#     ],
# )
# def test_concrete_pipe_interface(
#     pipe: Callable, expected: PipeInterface
# ):
#     dfi = PipeInterface.from_pipe_definition(pipe)
#     dfi.connect_upstream(upstream)
#     dfi.resolve_otypes(env)
#     assert dfi == expected


def test_inputs():
    env = make_test_env()
    n1 = env.add_node("node1", df_t1_source)
    n2 = env.add_node("node2", df_t1_to_t2, inputs={"input": "node1"})
    dfi = n2.get_interface()
    assert dfi is not None
    n3 = env.add_node("node3", df_chain_t1_to_t2, inputs="node1")
    dfi = n3.get_interface()
    assert dfi is not None


# def test_stream_input():
#     env = make_test_env()
#     env.add_module(core)
#     n1 = env.add_node("node1", df_t1_source)
#     n2 = env.add_node("node2", df_t1_source)
#     n3 = env.add_node("node3", df_chain_t1_to_t2, inputs="node1")
#     ds1 = env.add_node(
#         "ds1",
#         accumulate_as_dataset,
#         config=dict(dataset_name="type1"),
#         upstream=DataBlockStream(otype="TestType1"),
#     )
#     dfi = ds1.get_interface()


def test_python_pipe():
    env = make_test_env()
    df = pipe(df_t1_sink)
    assert (
        df.name == df_t1_sink.__name__
    )  # TODO: do we really want this implicit name? As long as we error on duplicate should be ok

    k = "name1"
    df = pipe(df_t1_sink, name=k)
    assert df.name == k

    dfi = df.get_interface(env)
    assert dfi is not None


# def test_sql_pipe():
#     env = make_test_env()
#     sql = "select:T 1 from t:T"
#     k = "k1"
#     df = sql_pipe(k, sql)
#     assert df.name == k
#
#     dfi = df.get_interface(env)
#     assert dfi is not None
#
#     assert len(dfi.inputs) == 1
#     assert dfi.inputs[0].otype_like == "T"
#     assert dfi.inputs[0].name == "t"
#     assert dfi.output is not None
#     assert dfi.output.otype_like == "T"
#
#
# def test_sql_pipe2():
#     env = make_test_env()
#     sql = "select:T 1 from from t1:U join t2:Optional[T]"
#     df = sql_pipe("s1", sql)
#     dfi = df.get_interface(env)
#     assert dfi is not None
#
#     assert len(dfi.inputs) == 2
#     assert dfi.inputs[0].otype_like == "U"
#     assert dfi.inputs[0].name == "t1"
#     assert not dfi.inputs[0].is_optional
#     assert dfi.inputs[1].otype_like == "T"
#     assert dfi.inputs[1].name == "t2"
#     assert dfi.inputs[1].is_optional


def test_sql_pipe_interface():
    env = make_test_env()
    sql = """select 1 from from t1 -- DataBlock[T1]
    join t2 on t1.a = t2.b left join t3 -- DataSet[T2]
    on"""
    df = sql_pipe("s1", sql)
    dfi = df.get_interface(env)
    assert dfi is not None

    assert len(dfi.inputs) == 3
    assert dfi.inputs[0].otype_like == "T1"
    assert dfi.inputs[0].name == "t1"
    assert dfi.inputs[0].data_format_class == "DataBlock"
    assert dfi.inputs[1].otype_like == "Any"
    assert dfi.inputs[1].name == "t2"
    assert dfi.inputs[2].otype_like == "T2"
    assert dfi.inputs[2].name == "t3"
    assert dfi.inputs[2].data_format_class == "DataSet"
    assert dfi.output is not None

    sql = """select -- DataSet[T]
    1 
    from -- comment inbetween
    input
    join t2 on t1.a = t2.b"""
    df = sql_pipe("s1", sql)
    dfi = df.get_interface(env)
    assert dfi is not None
    assert len(dfi.inputs) == 2
    assert dfi.output is not None
    assert dfi.output.otype_like == "T"
    assert dfi.output.data_format_class == "DataSet"

    sql = """select 1, 'not a commment -- nope'
    from -- comment inbetween
    t1, t2 on t1.a = t2.b"""
    df = sql_pipe("s1", sql)
    dfi = df.get_interface(env)
    assert dfi is not None
    assert len(dfi.inputs) == 2

    sql = """select 1, 'not a commment -- nope'
    from {% jinja block %}
    t1, t2 on t1.a = t2.b"""
    df = sql_pipe("s1", sql)
    dfi = df.get_interface(env)
    assert dfi is not None
    assert len(dfi.inputs) == 2

    sql = """select 1, 'not a commment -- nope'
    from {% jinja block %}
    this"""
    df = sql_pipe("s1", sql, inputs={"this": "Optional[DataBlock[T]]"})
    dfi = df.get_interface(env)
    assert dfi is not None
    assert len(dfi.inputs) == 1
    assert dfi.inputs[0].data_format_class == "DataBlock"
    assert dfi.inputs[0].name == "this"
    assert dfi.inputs[0].is_self_ref
    assert dfi.inputs[0].is_optional
    assert dfi.inputs[0].is_generic

    sql = """
            select -- DataBlock[T]
            {% if inputs.input.realized_otype.unique_on %}
                distinct on (
                    {% for col in inputs.input.realized_otype.unique_on %}
                        "{{ col }}"
                        {%- if not loop.last %},{% endif %}
                    {% endfor %}
                    )
            {% endif %}
                {% for col in inputs.input.realized_otype.fields %}
                    "{{ col.name }}"
                    {%- if not loop.last %},{% endif %}
                {% endfor %}

            from input -- DataBlock[T]
            {% if inputs.input.resolved_otype.updated_at_field %}
            order by
                {% for col in inputs.input.realized_otype.unique_on %}
                    "{{ col }}",
                {% endfor %}
                "{{ inputs.input.resolved_otype.updated_at_field.name }}" desc
            {% endif %}
    """
    df = sql_pipe("s1", sql)
    dfi = df.get_interface(env)
    assert dfi is not None
    assert len(dfi.inputs) == 1
    assert dfi.output is not None


# def test_table_refs():
#     s = """
#     select *
#     from (
#     select * from a, b
#     on a=b) s
#     left join -- commment
#     c
#     where
#     c = 'select * from nope';
#     """
#
#     table_refs = find_all_table_references(s)[0]
#     assert table_refs == ["a", "b", "c"]


@pipe("k1", compatible_runtimes="python")
def df1():
    pass


@pipe("k1", compatible_runtimes="mysql")
def df2():
    pass


# def test_pipe_registry():
#     r = PipeRegistry()
#     dfs = Pipe(name="k1", module_name=DEFAULT_MODULE_NAME, version=None)
#     dfs.add_definition(df1)
#     r.process_and_register_all([df_t1_sink, df_chain_t1_to_t2, dfs, df2])
#     assert r.get("k1") is dfs
#     assert r.get("k1").runtime_pipes[RuntimeClass.PYTHON] is df1
#     assert r.get("k1").runtime_pipes[RuntimeClass.DATABASE] is df2


def test_node_no_inputs():
    env = make_test_env()
    df = pipe(df_t1_source)
    node1 = Node(env, "node1", df)
    assert {node1: node1}[node1] is node1  # Test hash
    dfi = node1.get_interface()
    assert dfi.inputs == []
    assert dfi.output is not None
    assert node1.get_declared_inputs() == {}
    assert not node1.is_composite()


def test_node_inputs():
    env = make_test_env()
    df = pipe(df_t1_source)
    node = Node(env, "node", df)
    df = pipe(df_t1_sink)
    with pytest.raises(Exception):
        # Bad input
        Node(env, "node_fail", df, input="Turname")  # type: ignore
    node1 = Node(env, "node1", df, inputs=node)
    dfi = node1.get_interface()
    dfi = node1.get_interface()
    assert len(dfi.inputs) == 1
    assert dfi.output is None
    assert list(node1.get_declared_inputs().keys()) == ["input"]
    # assert node1.get_input("input").get_upstream(env)[0] is node


def test_node_config():
    env = make_test_env()
    config_vals = []

    def df_ctx(ctx: PipeContext):
        config_vals.append(ctx.get_config("test"))

    ctx = env.add_node("ctx", df_ctx, config={"test": 1, "extra_arg": 2})
    with env.execution() as exe:
        exe.run(ctx)
    assert config_vals == [1]


# def test_node_chain():
#     env = make_test_env()
#     df = pipe(df_t1_source)
#     node = Node(env, "node", df)
#     node1 = PipeNodeChain(env, "node1", df_chain_t1_to_t2, upstream=node)
#     dfi = node1.get_interface()
#     assert len(dfi.inputs) == 1
#     assert dfi.output is not None
#     assert list(node1.get_inputs().keys()) == ["input"]
#     assert node1.get_input("input").get_upstream(env)[0] is node
#     # Output NODE
#     output_node = node1.get_output_node()
#     assert output_node is not node1
#     assert node1.name in output_node.name
#     out_dfi = output_node.get_interface()
#     assert len(out_dfi.inputs) == 1
#     assert out_dfi.output is not None
#     # Children
#     assert len(node1.get_nodes()) == 2


# def test_graph_resolution():
#     env = make_test_env()
#     n1 = env.add_node("node1", df_t1_source)
#     n2 = env.add_node("node2", df_t1_source)
#     n3 = env.add_node("node3", df_chain_t1_to_t2, upstream="node1")
#     n4 = env.add_node("node4", df_t1_to_t2, upstream="node2")
#     n5 = env.add_node("node5", df_generic, upstream="node4")
#     n6 = env.add_node("node6", df_self, upstream="node4")
#     fgr = PipeGraphResolver(env)
#     # Resolve types
#     assert fgr.resolve_output_type(n4) is TestType2
#     assert fgr.resolve_output_type(n5) is TestType2
#     assert fgr.resolve_output_type(n6) is TestType2
#     fgr.resolve_output_types()
#     last = n3.get_nodes()[-1]
#     assert fgr._resolved_output_types[last] is TestType2
#     # Resolve deps
#     assert fgr._resolve_node_dependencies(n4)[0].parent_nodes == [n2]
#     assert fgr._resolve_node_dependencies(n5)[0].parent_nodes == [n4]
#     fgr.resolve_dependencies()
#     # Otype resolution
#     n7 = env.add_node("node7", df_self, upstream=DataBlockStream(otype="TestType2"))
#     n8 = env.add_node("node8", df_self, upstream=n7)
#     fgr = env.get_pipe_graph_resolver()
#     fgr.resolve()
#     parent_keys = set(
#         p.name for p in fgr.get_resolved_interface(n7).inputs[0].parent_nodes
#     )
#     assert parent_keys == {
#         "node3__df_t1_to_t2",
#         "node3__df_generic",
#         "node4",
#         "node5",
#         "node6",
#     }


def test_any_otype_interface():
    env = make_test_env()
    env.add_module(core)

    def df_any(input: DataBlock) -> DataFrame:
        pass

    df = pipe(df_any)
    dfi = df.get_interface(env)
    assert dfi.inputs[0].otype_like == "Any"
    assert dfi.output.otype_like == "Any"
