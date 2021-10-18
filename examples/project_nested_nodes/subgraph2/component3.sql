select
    *
from {{ Table('cross_app_ref') }}
limit {{ Parameter('limit', 10)}}
