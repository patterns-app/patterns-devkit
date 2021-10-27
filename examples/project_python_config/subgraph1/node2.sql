{% python %}
node(
    display_options
)
{% endpython %}


select
    *
from {{ Table('inputtable') }}
limit {{ Parameter('limit', 10)}}
