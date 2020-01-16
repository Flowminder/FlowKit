from flowetl.mixins.fixed_sql_mixin import fixed_sql_operator

AttachOperator = fixed_sql_operator(
    class_name="AttachOperator",
    sql="""
        DROP TABLE IF EXISTS {{ final_table }};
        ALTER TABLE {{ extract_table }} RENAME TO {{ table_name }};
        ALTER TABLE {{ etl_schema }}.{{ table_name }} SET SCHEMA {{ final_schema }};
        ALTER TABLE {{ final_table }} INHERIT {{ parent_table }};
        """,
)
