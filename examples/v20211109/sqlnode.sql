create table {{ table('my_output') }} as
select 
    *
from
    {{ table1 }}
