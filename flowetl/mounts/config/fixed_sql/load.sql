ALTER TABLE {{transform_table}} RENAME TO {{load_table.split(".")[-1]}};
ALTER TABLE etl.{{load_table.split(".")[-1]}} SET SCHEMA events;
ALTER TABLE events.{{load_table.split(".")[-1]}} INHERIT events.{{cdr_type.name.lower()}};
