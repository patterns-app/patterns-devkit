-- TODO: this is no-op if "this" is empty... is there a way to shortcut?
-- TODO: does the bound stream thing even work?? Do we have a test somewhere?
-- TODO: what if we have mixed schemas? need explicit columns

{% if input_objects.previous.bound_block %}
select * from {{ inputs.previous }}
union all
{% endif %}
{% for block in input_objects.new.bound_stream %}
select
* from {{ block.as_sql_from_stmt(storage) }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}