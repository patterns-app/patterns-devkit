
create table {{ OutputTable("transactions_staging") }} as
select
    record -> 'id' as id
  , record -> 'amount' as amount
  , record -> 'customer_id' as customer_id
  , record -> 'timestamp' as timestamp
  , now() as flattened_at 
from {{ InputTable("transactions_raw") }}
