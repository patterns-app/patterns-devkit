select
    *
from {{ Table('inputtable') }}
limit {{ Parameter('limit', 10)}}
