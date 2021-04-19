-- TODO: is there a generic minimal ANSI sql solution to dedupe keep newest? hmmmm
--  Does not appear to be, only hacks that require the sort column to be unique

select:T
{% if input_objects.input.nominal_schema and input_objects.input.nominal_schema.unique_on %}
    distinct on (
        {% for col in input_objects.input.nominal_schema.unique_on %}
            "{{ col }}"
            {%- if not loop.last %},{% endif %}
        {% endfor %}
        )
{% endif %}
    {% for col in input_objects.input.realized_schema.fields %}
        "{{ col.name }}"
        {%- if not loop.last %},{% endif %}
    {% endfor %}

from input:T
{% if input_objects.input.nominal_schema.updated_at_field %}
order by
    {% for col in input_objects.input.nominal_schema.unique_on %}
        "{{ col }}",
    {% endfor %}
    "{{ input_objects.input.nominal_schema.updated_at_field.name }}" desc
{% endif %}
