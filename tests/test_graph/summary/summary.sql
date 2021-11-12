    with mycte as (
        
        )

    create table {{ table("summary_table") }} as
select * from (
                  select


                  from {{ table("random_floats", schema="Transaction") }} as floats
join {{ table("other_outputtable") }} as other
                  on floats.customer_id = other.id
                      window
                  where
                      limit {{ parameter("limit", "int", default=100) }}
                        
                  order by
              )