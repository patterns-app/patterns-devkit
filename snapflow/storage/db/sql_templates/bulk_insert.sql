INSERT INTO "{{ table_name }}" (
    {{ columns|column_list }}
) VALUES
    %s
{% if ignore_duplicates %}
on conflict ({{ unique_on_column }}) do nothing
{% endif %}
;
