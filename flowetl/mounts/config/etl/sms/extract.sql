DROP TABLE IF EXISTS {{ get_extract_view(ds_nodash) }};
CREATE TABLE {{ get_extract_view(ds_nodash) }} (LIKE events.{{cdr_type}} INCLUDING ALL)
