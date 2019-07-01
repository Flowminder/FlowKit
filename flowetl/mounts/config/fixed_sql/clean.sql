DROP TABLE IF EXISTS {{ dag_run.conf.extract_table }};
DROP TABLE IF EXISTS {{ dag_run.conf.transform_table }};
