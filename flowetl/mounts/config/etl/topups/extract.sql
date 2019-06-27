DROP TABLE IF EXISTS {{ extract_table }};
CREATE TABLE {{ extract_table }} (LIKE events.{{cdr_type.name.lower()}} INCLUDING ALL)
