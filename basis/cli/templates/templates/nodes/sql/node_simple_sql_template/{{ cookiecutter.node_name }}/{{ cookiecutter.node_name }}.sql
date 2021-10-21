select
    *
from {{ "{{" }} Table("other_node") {{ "}}" }}
limit {{ "{{" }} Parameter("limit", "int", default=100) {{ "}}" }}