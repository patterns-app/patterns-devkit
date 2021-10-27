select
    *
from {{ Table('customers') }}
limit {{ Parameter('limit', 10)}}