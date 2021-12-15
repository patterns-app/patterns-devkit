
create table {{ OutputTable("monthly_revenue") }} as
select
    date_trunc('month', timestamp) as year_month
  , sum(amount) as revenue
from {{ InputTable("transactions_staging") }}
group by 1
