select
    *
from {{ Table('customer_summary_table') }}
limit {{ Parameter('limit', 10)}}