from __future__ import annotations

from basis.core.declarative.function import (
    DEFAULT_OUTPUTS,
    BlockType,
    FunctionInterfaceCfg,
    IoBaseCfg,
    Parameter,
)
from basis.core.sql.jinja import parse_interface_from_sql, render_sql
from basis.core.sql.sql_function import (
    ParsedSqlStatement,
    extract_tables,
    sql_function,
    sql_function_factory,
)
from tests.utils import make_test_env


def test_sql_parse_new_style_jinja():
    sql = """
    select * from {{ Record("orders", "TestSchema") }}
    join {{ Table("customers") }}
    where col = {{ Parameter("p1", datatype="text", default=0) }}
    """
    dfi = parse_interface_from_sql(sql)
    assert dfi == FunctionInterfaceCfg(
        inputs={
            "orders": IoBaseCfg(
                name="orders", schema="TestSchema", block_type=BlockType.Record,
            ),
            "customers": IoBaseCfg(name="customers", block_type=BlockType.Table,),
        },
        outputs=DEFAULT_OUTPUTS,
        parameters={
            "p1": Parameter(name="p1", datatype="str", default=0)
        },  # TODO: required ... with default?
    )


def test_sql_render_new_style_jinja():
    sql = """
    select * from {{ Table("orders") }}
    where col = {{ Parameter("p1", "text", default=0) }}
    """
    rendered = render_sql(sql, dict(orders="orders_table"), dict(p1="'val1'"),)
    expected = """
    select * from orders_table
    where col = 'val1'
    """
    assert rendered == expected


def test_sql_find_tables():
    sql = """select 1 from t1
        join t2
        -- unrelated comment with a colon: in it
        where {{ Parameter("param1")}}
        """
    parsed = extract_tables(sql)
    expected_sql = """select 1 from {{ Table("t1") }} as t1
        join {{ Table("t2") }} as t2
        -- unrelated comment with a colon: in it
        where {{ Parameter("param1")}}
        """
    expected_tables = {"t1": "t1", "t2": "t2"}
    assert parsed == ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars=expected_sql,
        found_tables=expected_tables,
    )


def test_sql_find_tables_subquery():
    sql = """select 1 from t1
        JOIN (select 1) AS b
        -- unrelated comment with a colon: in it
        where {{ Parameter("param1")}}
        """
    parsed = extract_tables(sql)
    expected_sql = """select 1 from {{ Table("t1") }} as t1
        JOIN (select 1) AS b
        -- unrelated comment with a colon: in it
        where {{ Parameter("param1")}}
        """
    expected_tables = {"t1": "t1"}
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
        where {{ Parameter("param1")}}
        """
    parsed = extract_tables(sql)
    expected_sql = """
        with t1 as (select 1)
        select 1 from t1
        join {{ Table("t2") }} as t2
        -- unrelated comment with a colon: in it
        where {{ Parameter("param1")}}
        """
    expected_tables = {"t2": "t2"}
    assert parsed == ParsedSqlStatement(
        original_sql=sql,
        sql_with_jinja_vars=expected_sql,
        found_tables=expected_tables,
    )


def test_sql_function_interface():
    sql = """select 1 from t1
        join t2 on t1.a = t2.b left join t3
        on"""
    df = sql_function_factory("s1", sql)
    pi = df.get_interface()
    assert pi is not None

    assert len(pi.inputs) == 3
    t1 = pi.get_input("t1")
    t2 = pi.get_input("t2")
    t3 = pi.get_input("t3")
    assert t1.schema_key == None
    assert t1.name == "t1"
    assert t1.block_type == BlockType.Table
    assert t2.schema_key == None
    assert t2.name == "t2"
    assert t3.schema_key == None
    assert t3.name == "t3"
    assert t3.block_type == BlockType.Table
    assert pi.get_default_output() is not None


# def test_sql_function_interface_fn_no_autodetect():
#     @Sql(autodetect_inputs=False)
#     def sfunction():
#         sql = """select 1 from from t1:T1
#             join t2 on t1.a = t2.b left join t3:T2
#             on"""
#         return sql

#     pi = sfunction.get_interface()
#     assert pi is not None

#     # We'll miss `t2` with no autodetect, so just two
#     assert len(pi.inputs) == 2


# def test_sql_function_interface_fn_function():
#     @sql_function
#     def sfunction():
#         sql = """select 1 from from t1:T1
#             join t2:Any on t1.a = t2.b left join t3:T2
#             on"""
#         return sql

#     pi = sfunction.get_interface()
#     assert pi is not None

#     assert len(pi.inputs) == 3

#     @sql_function(autodetect_inputs=False, name="newname")
#     def snp():
#         sql = """select 1 from from t1:T1
#             join t2 on t1.a = t2.b left join t3:T2
#             on"""
#         return sql

#     pi = snp.get_interface()
#     assert pi is not None
#     assert snp.name == "newname"

#     assert len(pi.inputs) == 2


# def test_sql_function_interface_output():
#     sql = """select:Block[T]
#         1
#         from -- comment inbetween
#         input
#         join t2 on t1.a = t2.b"""
#     df = sql_function("s1", sql)
#     pi = df.get_interface()
#     assert pi is not None
#     assert len(pi.inputs) == 2
#     assert pi.get_default_output() is not None
#     assert pi.get_default_output().schema_key == "T"
#     assert (
#         pi.get_default_output().data_format is None
#     )  # TODO: is this what we want? or is it "Any" by default?


# Don't support aliases
# def test_sql_function_interface_table_alias():
#     sql = """select:Block[T]
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
#     assert pi.get_default_output().data_format == "Block"


def test_sql_function_interface_comment_like_string():
    sql = """select 1, 'not a commment -- nope'
        from -- comment inbetween
        t1, t2 on t1.a = t2.b"""
    df = sql_function_factory("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 2


def test_sql_function_interface_jinja_block():
    sql = """select 1, 'not a commment -- nope'
        from {% if False %}{% endif %}
        t1, t2 on t1.a = t2.b"""
    df = sql_function_factory("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 2


def test_sql_function_interface_self_ref():
    sql = """select 1, 'not a commment -- nope'
        from {% if False %}{% endif %}
        this"""
    df = sql_function_factory("s1", sql)
    pi = df.get_interface()
    assert pi is not None
    assert len(pi.inputs) == 1
    assert pi.get_single_input().block_type == BlockType.Table
    assert pi.get_single_input().name == "this"
    # assert pi.get_single_non_recursive_input().from_self
    # assert not pi.get_single_input().required
    # assert (
    #     not pi.get_single_input().is_generic
    # )  # TODO: schema has to be same as default output


# def test_sql_function_interface_complex_jinja():
#     sql = """
#         select:T
#         {% if inputs.input.realized_schema.unique_on %}
#             distinct on (
#                 {% for col in inputs.input.realized_schema.unique_on %}
#                     "{{ col }}"
#                     {%- if not loop.last %},{% endif %}
#                 {% endfor %}
#                 )
#         {% endif %}
#             {% for col in inputs.input.realized_schema.fields %}
#                 "{{ col.name }}"
#                 {%- if not loop.last %},{% endif %}
#             {% endfor %}

#         from input:T
#         {% if inputs.input.resolved_schema.updated_at_field %}
#         order by
#             {% for col in inputs.input.realized_schema.unique_on %}
#                 "{{ col }}",
#             {% endfor %}
#             "{{ inputs.input.resolved_schema.updated_at_field.name }}" desc
#         {% endif %}"""
#     df = sql_function("s1", sql)
#     pi = df.get_interface()
#     assert pi is not None
#     assert len(pi.inputs) == 1
#     assert pi.get_single_input().is_generic
#     assert pi.get_single_input().schema_key == "T"
#     assert pi.get_default_output().is_generic
#     assert pi.get_default_output() is not None
