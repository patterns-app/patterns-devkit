create table {{ table("summary_table") }} as
select
    *
from {{ stripe_charges_table }}
