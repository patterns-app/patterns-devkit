copy {{ output_table }} (
 {{ output_object_type.field_name_list|join(",") }}
)
from '{{ csv_path }}' delimiter ',' csv header
;
