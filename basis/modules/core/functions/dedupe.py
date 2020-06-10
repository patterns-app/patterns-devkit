from basis.core.sql.data_function import sql_datafunction

dedupe_unique_keep_first_value = sql_datafunction(
    name="dedupe_unique_keep_first_value",
    compatible_runtimes="postgres",
    sql="""
select:T
distinct on (
    {% for col in inputs.input.otype.unique_on %}
        "{{ col }}"
        {%- if not loop.last %},{% endif %}
    {% endfor %}
    )
    {% for col in inputs.input.otype.fields %}
        "{{ col.name }}"
        {%- if not loop.last %},{% endif %}
    {% endfor %}

from input:T
""",
)
