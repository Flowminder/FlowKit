ALTER TABLE {{get_transform_table(ds_nodash)}} RENAME TO {{get_load_table(ds_nodash).split(".")[-1]}};
ALTER TABLE etl.{{get_load_table(ds_nodash).split(".")[-1]}} SET SCHEMA events;
ALTER TABLE events.{{get_load_table(ds_nodash).split(".")[-1]}} INHERIT events.{{cdr_type}};
