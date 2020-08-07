from __future__ import annotations

from basis.core.sql.pipe import sql_pipe
from basis.testing.pipes import PipeTest

# dedupe_unique_keep_max_value = sql_pipe(
#     name="dedupe_unique_keep_max_value",
#     sql="""
# select:T
# distinct on (
#     {% for col in inputs.input.realized_otype.fields %}
#         {% if col.name in inputs.input.realized_otype.unique_on %}
#             "{{ col.name }}"
#         {% else %}
#             {% if col.field_type.startswith("Bool") %}
#                 bool_or("{{ col.name }}") as "{{ col.name }}"
#             {% else %}
#                 max("{{ col.name }}") as "{{ col.name }}"
#             {% endif %}
#         {% endif -%}
#         {%- if not loop.last %},{% endif %}
#     {% endfor %}
# from input:T
# group by
#     {% for col in inputs.input.realized_otype.unique_on -%}
#         "{{ col }}"
#         {%- if not loop.last %},{% endif %}
#     {% endfor %}
# """,
# )


# dedupe_unique_keep_first_value = sql_pipe(
#     name="dedupe_unique_keep_first_value",
#     compatible_runtimes="postgres",
#     sql="""
# select:T
# distinct on (
#     {% for col in inputs.input.realized_otype.unique_on %}
#         "{{ col }}"
#         {%- if not loop.last %},{% endif %}
#     {% endfor %}
#     )
#     {% for col in inputs.input.realized_otype.fields %}
#         "{{ col.name }}"
#         {%- if not loop.last %},{% endif %}
#     {% endfor %}
#
# from input:T
# """,
# )


# TODO: currently no-op when no unique columns specified, should probably be ALL columns
#   but _very_ expensive. In general any deduping on non-indexed columns will be costly.
dedupe_unique_keep_newest_row = sql_pipe(
    name="dedupe_unique_keep_newest_row",
    compatible_runtimes="postgres",
    sql="""
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
""",
)


dedupe_test = PipeTest(
    pipe="dedupe_unique_keep_newest_row",
    tests=[
        {
            "name": "test_dupe",
            "test_data": {
                "input": {
                    "otype": "CoreTestType",
                    "data": """
                        k1,k2,f1,f2,f3,f4
                        1,2,abc,1.1,1,2012-01-01
                        1,2,def,1.1,{"1":2},2012-01-02
                        1,3,abc,1.1,2,2012-01-01
                        1,4,,,"[1,2,3]",2012-01-01
                        2,2,1.0,2.1,"[1,2,3]",2012-01-01
                    """,
                },
                "output": {
                    "otype": "CoreTestType",
                    "data": """
                        k1,k2,f1,f2,f3,f4
                        1,2,def,1.1,{"1":2},2012-01-02
                        1,3,abc,1.1,2,2012-01-01
                        1,4,,,"[1,2,3]",2012-01-01
                        2,2,1.0,2.1,"[1,2,3]",2012-01-01
                    """,
                },
            },
        }
    ],
)
