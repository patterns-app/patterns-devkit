from __future__ import annotations

from snapflow.api import Input
from snapflow.core.declarative.function import (
    DataFunctionInputCfg,
    DataFunctionInterfaceCfg,
)
from snapflow.core.function_interface import DEFAULT_OUTPUTS, InputType, Parameter
from snapflow.core.sql.parser import parse_interface_from_sql, render_sql
from snapflow.core.sql.sql_function import (
    AnnotatedParam,
    AnnotatedSqlTable,
    ParsedSqlStatement,
    Sql,
    extract_param_annotations,
    extract_table_annotations,
    extract_tables,
    sql_datafunction,
    sql_function,
)
from tests.utils import make_test_env


"""
Future test cases

select:T
from previous:SelfReference[T] -- Previous output
-- from new:Stream[T] -- with comment
with error:T -- comment
with state:State

"""


def test_sql_parse_params():
    sql = """select 1 from from t1:Schema1
        -- unrelated comment with a colon: in it
        where :param1:dtype1
        and :param2
        """
    parsed = extract_param_annotations(sql)
    expected_sql = """select 1 from from t1:Schema1
        -- unrelated comment with a colon: in it
        where {{ params['param1'] }}
        and {{ params['param2'] }}
        """
    expected_params = {
        "param1": AnnotatedParam(name="param1", annotation="dtype1"),
        "param2": AnnotatedParam(name="param2"),
    }
    assert parsed == ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars=expected_sql,
        found_params=expected_params,
    )
    sql = " not a:param"
    parsed = extract_param_annotations(sql)
    assert not parsed.found_params


def test_sql_parse_tables_complex():
    sql = """
        {% if input_objects.previous.bound_block %}
        select:T
            *
        from previous:SelfReference[T]
        union all
        {% endif %}
        {% for block in input_objects.new.bound_stream %}
        select
            *
        -- from new:Stream[T]   <-- REQUIRED mock annotation for interface detection
        from {{ block.as_sql_from_stmt(storage) }}
        {% if not loop.last %}
        union all
        {% endif %}
        {% endfor %}
        """
    parsed = extract_table_annotations(sql)
    expected_tables = {
        "previous": AnnotatedSqlTable(name="previous", annotation="SelfReference[T]"),
        "new": AnnotatedSqlTable(name="new", annotation="Stream[T]"),
    }
    assert parsed.found_tables == expected_tables


def test_sql_parse_tables():
    sql = """select 1 from from t1:Schema1
        join t2:Schema2
        -- unrelated comment with a colon: in it
        where :param1:dtype1
        """
    parsed = extract_table_annotations(sql)
    expected_sql = """select 1 from from {{ inputs['t1'] }} as t1
        join {{ inputs['t2'] }} as t2
        -- unrelated comment with a colon: in it
        where :param1:dtype1
        """
    expected_tables = {
        "t1": AnnotatedSqlTable(name="t1", annotation="Schema1"),
        "t2": AnnotatedSqlTable(name="t2", annotation="Schema2"),
    }
    assert parsed == ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars=expected_sql,
        found_tables=expected_tables,
    )


def test_sql_parse_new_style_jinja():
    sql = """
    select * from {% input orders Stream[TestSchema] %}
    join {% input customers %}
    where col = {% param p1 text 0 %}
    """
    dfi = parse_interface_from_sql(sql)
    assert dfi == DataFunctionInterfaceCfg(
        inputs={
            "orders": DataFunctionInputCfg(
                name="orders",
                schema_key="TestSchema",
                input_type=InputType.Stream,
            ),
            "customers": DataFunctionInputCfg(
                name="customers",
                schema_key="Any",
                input_type=InputType.Reference,
            ),
        },
        outputs=DEFAULT_OUTPUTS,
        parameters={
            "p1": Parameter(name="p1", datatype="str", default=0, required=True)
        },  # TODO: required ... with default?
        uses_context=True,
    )


def test_sql_render_new_style_jinja():
    sql = """
    select * from {% input orders %}
    where col = {% param p1 text 0 %}
    """
    rendered = render_sql(
        sql,
        dict(orders="orders_table"),
        dict(p1="'val1'"),
    )
    expected = """
    select * from orders_table
    where col = 'val1'
    """
    assert rendered == expected


def test_sql_find_tables():
    sql = """select 1 from t1
        join t2
        -- unrelated comment with a colon: in it
        where :param1:dtype1
        """
    parsed = extract_tables(sql)
    expected_sql = """select 1 from {{ inputs['t1'] }} as t1
        join {{ inputs['t2'] }} as t2
        -- unrelated comment with a colon: in it
        where :param1:dtype1
        """
    expected_tables = {
        "t1": AnnotatedSqlTable(name="t1"),
        "t2": AnnotatedSqlTable(name="t2"),
    }
    assert parsed == ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars=expected_sql,
        found_tables=expected_tables,
    )


def test_sql_find_tables_subquery():
    sql = """select 1 from t1
        JOIN (select 1) AS b
        -- unrelated comment with a colon: in it
        where :param1:dtype1
        """
    parsed = extract_tables(sql)
    expected_sql = """select 1 from {{ inputs['t1'] }} as t1
        JOIN (select 1) AS b
        -- unrelated comment with a colon: in it
        where :param1:dtype1
        """
    expected_tables = {
        "t1": AnnotatedSqlTable(name="t1"),
    }
    assert parsed == ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars=expected_sql,
        found_tables=expected_tables,
    )


def test_sql_find_tables_with_statement():
    # WITH statement
    sql = """
        with t1 as (select 1)
        select 1 from t1
        join t2
        -- unrelated comment with a colon: in it
        where :param1:dtype1
        """
    parsed = extract_tables(sql)
    expected_sql = """
        with t1 as (select 1)
        select 1 from t1
        join {{ inputs['t2'] }} as t2
        -- unrelated comment with a colon: in it
        where :param1:dtype1
        """
    expected_tables = {
        "t2": AnnotatedSqlTable(name="t2"),
    }
    assert parsed == ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars=expected_sql,
        found_tables=expected_tables,
    )


def test_sql_function_interface():
    sql = """select 1 from t1:T1
        join t2:Any on t1.a = t2.b left join t3:T2
        on"""
    df = sql_function("s1", sql)
    pi = df.get_interface()
    assert pi is not None

    assert len(pi.inputs) == 3
    t1 = pi.get_input("t1")
    t2 = pi.get_input("t2")
    t3 = pi.get_input("t3")
    assert t1.schema_key == "T1"
    assert t1.name == "t1"
    assert t1.input_type == InputType("DataBlock")
    assert t2.schema_key == "Any"
    assert t2.name == "t2"
    assert t3.schema_key == "T2"
    assert t3.name == "t3"
    assert t3.input_type == InputType("DataBlock")
    assert pi.get_default_output() is not None


def test_sql_function_interface_fn():
    @Sql
    def sfunction():
        sql = """select 1 from from t1:T1
            join t2:Any on t1.a = t2.b left join t3:T2
            on"""
        return sql

    pi = sfunction.get_interface()
    assert pi is not None

    assert len(pi.inputs) == 3
    t1 = pi.get_input("t1")
    t2 = pi.get_input("t2")
    t3 = pi.get_input("t3")
    assert t1.schema_key == "T1"
    assert t1.name == "t1"
    assert t1.input_type == InputType("DataBlock")
    assert t2.schema_key == "Any"
    assert t2.name == "t2"
    assert t3.schema_key == "T2"
    assert t3.name == "t3"
    assert t3.input_type == InputType("DataBlock")
    assert pi.get_default_output() is not None


def test_sql_function_interface_fn_no_autodetect():
    @Sql(autodetect_inputs=False)
    def sfunction():
        sql = """select 1 from from t1:T1
            join t2 on t1.a = t2.b left join t3:T2
            on"""
        return sql

    pi = sfunction.get_interface()
    assert pi is not None

    # We'll miss `t2` with no autodetect, so just two
    assert len(pi.inputs) == 2


def test_sql_function_interface_fn_function():
    @sql_datafunction
    def sfunction():
        sql = """select 1 from from t1:T1
            join t2:Any on t1.a = t2.b left join t3:T2
            on"""
        return sql

    pi = sfunction.get_interface()
    assert pi is not None

    assert len(pi.inputs) == 3

    @sql_datafunction(autodetect_inputs=False, name="newname")
    def snp():
        sql = """select 1 from from t1:T1
            join t2 on t1.a = t2.b left join t3:T2
            on"""
        return sql

    pi = snp.get_interface()
    assert pi is not None
    assert snp.name == "newname"

    assert len(pi.inputs) == 2


def test_sql_function_interface_output():
    sql = """select:DataBlock[T]
        1
        from -- comment inbetween
        input
        join t2 on t1.a = t2.b"""
    df = sql_function("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 2
    assert pi.get_default_output() is not None
    assert pi.get_default_output().schema_key == "T"
    assert (
        pi.get_default_output().data_format is None
    )  # TODO: is this what we want? or is it "Any" by default?


# Don't support aliases
# def test_sql_function_interface_table_alias():
#     sql = """select:DataBlock[T]
#         1
#         from -- comment inbetween
#         input as
#             i
#         join t2 as i2 on i.a = i2.b"""
#     df = sql_function("s1", sql)
#     pi = df.get_interface()
#     assert pi is not None
#     assert len(pi.inputs) == 2
#     assert pi.get_default_output() is not None
#     assert pi.get_default_output().schema_key == "T"
#     assert pi.get_default_output().data_format == "DataBlock"


def test_sql_function_interface_comment_like_string():
    sql = """select 1, 'not a commment -- nope'
        from -- comment inbetween
        t1, t2 on t1.a = t2.b"""
    df = sql_function("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 2


def test_sql_function_interface_jinja_block():
    sql = """select 1, 'not a commment -- nope'
        from {% jinja block %}
        t1, t2 on t1.a = t2.b"""
    df = sql_function("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 2


def test_sql_function_interface_self_ref():
    sql = """select 1, 'not a commment -- nope'
        from {% jinja block %}
        this:SelfReference"""
    df = sql_function("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 1
    assert pi.get_single_input().input_type == InputType.SelfReference
    assert pi.get_single_input().name == "this"
    # assert pi.get_single_non_recursive_input().from_self
    assert not pi.get_single_input().required
    assert (
        not pi.get_single_input().is_generic
    )  # TODO: schema has to be same as default output


def test_sql_function_interface_complex_jinja():
    sql = """
        select:T
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

        from input:T
        {% if inputs.input.resolved_schema.updated_at_field %}
        order by
            {% for col in inputs.input.realized_schema.unique_on %}
                "{{ col }}",
            {% endfor %}
            "{{ inputs.input.resolved_schema.updated_at_field.name }}" desc
        {% endif %}"""
    df = sql_function("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 1
    assert pi.get_single_non_reference_input().is_generic
    assert pi.get_single_non_reference_input().schema_key == "T"
    assert pi.get_default_output().is_generic
    assert pi.get_default_output() is not None