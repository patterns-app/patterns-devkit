from __future__ import annotations

from snapflow.core.sql.pipe import sql_pipe
from tests.utils import make_test_env


def test_sql_pipe_interface():
    sql = """select 1 from from t1 -- :DataBlock[T1]
        join t2 on t1.a = t2.b left join t3 -- :DataBlock[T2]
        on"""
    df = sql_pipe("s1", sql)
    pi = df.get_interface()
    assert pi is not None

    assert len(pi.inputs) == 3
    assert pi.inputs[0].schema_like == "T1"
    assert pi.inputs[0].name == "t1"
    assert pi.inputs[0].data_format_class == "DataBlock"
    assert pi.inputs[1].schema_like == "Any"
    assert pi.inputs[1].name == "t2"
    assert pi.inputs[2].schema_like == "T2"
    assert pi.inputs[2].name == "t3"
    assert pi.inputs[2].data_format_class == "DataBlock"
    assert pi.output is not None


def test_sql_pipe_interface_output():
    sql = """select -- :DataBlock[T]
        1
        from -- comment inbetween
        input
        join t2 on t1.a = t2.b"""
    df = sql_pipe("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 2
    assert pi.output is not None
    assert pi.output.schema_like == "T"
    assert pi.output.data_format_class == "DataBlock"


def test_sql_pipe_interface_table_alias():
    sql = """select -- :DataBlock[T]
        1
        from -- comment inbetween
        input as
            i
        join t2 as i2 on i.a = i2.b"""
    df = sql_pipe("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 2
    assert pi.output is not None
    assert pi.output.schema_like == "T"
    assert pi.output.data_format_class == "DataBlock"


def test_sql_pipe_interface_comment_like_string():
    sql = """select 1, 'not a commment -- nope'
        from -- comment inbetween
        t1, t2 on t1.a = t2.b"""
    df = sql_pipe("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 2


def test_sql_pipe_interface_jinja_block():
    sql = """select 1, 'not a commment -- nope'
        from {% jinja block %}
        t1, t2 on t1.a = t2.b"""
    df = sql_pipe("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 2


def test_sql_pipe_interface_self_ref():
    sql = """select 1, 'not a commment -- nope'
        from {% jinja block %}
        this"""
    df = sql_pipe("s1", sql, inputs={"this": "Optional[DataBlock[T]]"})
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 1
    assert pi.inputs[0].data_format_class == "DataBlock"
    assert pi.inputs[0].name == "this"
    assert pi.inputs[0].is_self_ref
    assert pi.inputs[0].is_optional
    assert pi.inputs[0].is_generic


def test_sql_pipe_interface_complex_jinja():
    sql = """
        select -- :DataBlock[T]
        {% if inputs.input.realized_schema.unique_on %}
            distinct on (
                {% for col in inputs.input.realized_schema.unique_on %}
                    "{{ col }}"
                    {%- if not loop.last %},{% endif %}
                {% endfor %}
                )
        {% endif %}
            {% for col in inputs.input.realized_schema.fields %}
                "{{ col.name }}"
                {%- if not loop.last %},{% endif %}
            {% endfor %}

        from input -- :DataBlock[T]
        {% if inputs.input.resolved_schema.updated_at_field %}
        order by
            {% for col in inputs.input.realized_schema.unique_on %}
                "{{ col }}",
            {% endfor %}
            "{{ inputs.input.resolved_schema.updated_at_field.name }}" desc
        {% endif %}"""
    df = sql_pipe("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 1
    assert pi.inputs[0].is_generic
    assert pi.inputs[0].schema_like == "T"
    assert pi.output.is_generic
    assert pi.output is not None
