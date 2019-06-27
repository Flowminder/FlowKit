DROP TABLE IF EXISTS {{ dag_run.conf.extract_table }};
CREATE TABLE {{ dag_run.conf.extract_table }} (LIKE events.{{dag_run.conf.cdr_type.name.lower()}} INCLUDING ALL)
