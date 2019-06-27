DROP TABLE IF EXISTS {{ extract_table(ds_nodash) }};
CREATE TABLE {{ extract_table(ds_nodash) }} (LIKE events.{{cdr_type}} INCLUDING ALL)
