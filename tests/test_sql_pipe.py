from __future__ import annotations

from dags.core.sql.pipe import sql_pipe
from tests.utils import make_test_env


def test_sql_pipe_interface():
    env = make_test_env()
    sql = """select 1 from from t1 -- :DataBlock[T1]
        join t2 on t1.a = t2.b left join t3 -- :DataSet[T2]
        on"""
    df = sql_pipe("s1", sql)
    dfi = df.get_interface(env)
    assert dfi is not None

    assert len(dfi.inputs) == 3
    assert dfi.inputs[0].schema_like == "T1"
    assert dfi.inputs[0].name == "t1"
    assert dfi.inputs[0].data_format_class == "DataBlock"
    assert dfi.inputs[1].schema_like == "Any"
    assert dfi.inputs[1].name == "t2"
    assert dfi.inputs[2].schema_like == "T2"
    assert dfi.inputs[2].name == "t3"
    assert dfi.inputs[2].data_format_class == "DataSet"
    assert dfi.output is not None

    sql = """select -- :DataSet[T]
        1
        from -- comment inbetween
        input
        join t2 on t1.a = t2.b"""
    df = sql_pipe("s1", sql)
    dfi = df.get_interface(env)
    assert dfi is not None
    assert len(dfi.inputs) == 2
    assert dfi.output is not None
    assert dfi.output.schema_like == "T"
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
    dfi = df.get_interface(env)
    assert dfi is not None
    assert len(dfi.inputs) == 1
    assert dfi.inputs[0].is_generic
    assert dfi.inputs[0].schema_like == "T"
    assert dfi.output.is_generic
    assert dfi.output is not None
