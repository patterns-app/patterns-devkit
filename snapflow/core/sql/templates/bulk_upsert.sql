create temp table "tmp_{{ table_name }}" as
    select * from "{{ table_name }}" limit 0;

insert into tmp_{{ table_name }} (
        {{ columns|column_list }}
    ) values
        %s
;


update "{{ table_name }}"
set
    {% for col in columns %}
        "{{ col }}" = sub."{{ col }}"
        {% if not loop.last %} , {% endif %}
    {% endfor %}
from (
        select * from "tmp_{{ table_name }}"
        where exists (
            select "{{ unique_on_column }}"
            from "{{ table_name }}"
            where "tmp_{{ table_name }}"."{{ unique_on_column }}"
                = "{{ table_name }}"."{{ unique_on_column }}"
        )
    ) as sub
where
    "{{ table_name }}"."{{ unique_on_column }}" = sub."{{ unique_on_column }}"
;


insert into "{{ table_name }}"
    select * from "tmp_{{ table_name }}"
    where not exists (
        select "{{ unique_on_column }}"
        from "{{ table_name }}"
        where "tmp_{{ table_name }}"."{{ unique_on_column }}"
            = "{{ table_name }}"."{{ unique_on_column }}"
    )
;

drop table "tmp_{{ table_name }}";
