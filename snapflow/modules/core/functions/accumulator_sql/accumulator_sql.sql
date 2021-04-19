-- TODO: this is no-op if "this" is empty... is there a way to shortcut?
-- TODO: what if we have mixed schemas? need explicit columns

{% if input_objects.previous.bound_block %}
select:T
    *
from previous:Consumable[T]
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
