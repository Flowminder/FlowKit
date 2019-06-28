ALTER TABLE {{dag_run.conf.transform_table}} RENAME TO {{dag_run.conf.load_table.split(".")[-1]}};
ALTER TABLE etl.{{dag_run.conf.load_table.split(".")[-1]}} SET SCHEMA events;
ALTER TABLE events.{{dag_run.conf.load_table.split(".")[-1]}} INHERIT events.{{dag_run.conf.cdr_type.name.lower()}};
