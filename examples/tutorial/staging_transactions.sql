
create table {{ OutputTable("transactions_staging") }} as
select
    record ->> 'id' as id
  , (record ->> 'amount')::float as amount
  , record ->> 'customer_id' as customer_id
  , (record ->> 'processed_at')::date as timestamp
  , now() as flattened_at 
from {{ InputTable("transactions_raw") }}
