-- Rename the transform table (which serves as a temporary target location for the ETL-ed data) to
-- its final name supplied by `get_load_table()` and set it as a child table of events.<cdr_type>.
ALTER TABLE {{get_transform_table(ds_nodash)}} RENAME TO {{get_load_table(ds_nodash).split(".")[-1]}};
ALTER TABLE etl.{{get_load_table(ds_nodash).split(".")[-1]}} SET SCHEMA events;
ALTER TABLE events.{{get_load_table(ds_nodash).split(".")[-1]}} INHERIT events.{{cdr_type}};
