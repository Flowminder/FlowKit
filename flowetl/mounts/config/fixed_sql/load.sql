ALTER TABLE {{transform_table(ds_nodash)}} RENAME TO {{load_table(ds_nodash).split(".")[-1]}};
ALTER TABLE etl.{{load_table(ds_nodash).split(".")[-1]}} SET SCHEMA events;
ALTER TABLE events.{{load_table(ds_nodash).split(".")[-1]}} INHERIT events.{{cdr_type}};
