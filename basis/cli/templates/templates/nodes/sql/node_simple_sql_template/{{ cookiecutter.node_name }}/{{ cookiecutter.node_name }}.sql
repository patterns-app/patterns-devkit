create table {{ "{{" }} OutputTable("my_output_table") {{ "}}" }} as
select
    *
from {{ "{{" }} InputTable("other_node") {{ "}}" }}
limit {{ "{{" }} Parameter("limit", "int", default=100) {{ "}}" }}
