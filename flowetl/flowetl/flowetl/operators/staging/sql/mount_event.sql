BEGIN;

CREATE SERVER IF NOT EXISTS csv_fdw FOREIGN DATA WRAPPER file_fdw;

DROP FOREIGN TABLE IF EXISTS {{params.event_type}}_table_{{ds_nodash}};
CREATE FOREIGN TABLE {{params.event_type}}_table_{{ds_nodash}}(
{% set comma = joiner(', ') %}
{% for col_name, dtype in params.column_dict[params.event_type].items() %}
    {{comma()}}{{col_name}} {{dtype}}
{% endfor %}
) SERVER csv_fdw
OPTIONS (filename '{{params.flowdb_csv_dir}}/{{ds_nodash}}_{{params.event_type}}.csv', format 'csv', header 'TRUE');