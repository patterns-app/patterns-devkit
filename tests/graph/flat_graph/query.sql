CREATE TABLE {{ OutputTable('sink_table') }}
SELECT * FROM {{ InputTable('query_table')}}
WHERE num > {{ Parameter('num', type='int') }}
