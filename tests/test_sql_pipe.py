from __future__ import annotations

from dags.core.sql.pipe import sql_pipe
from tests.utils import make_test_env


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
