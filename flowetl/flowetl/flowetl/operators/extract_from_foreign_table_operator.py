from flowetl.mixins.wrapping_sql_mixin import wrapped_sql_operator

ExtractFromForeignTableOperator = wrapped_sql_operator(
    class_name="ExtractFromForeignTableOperator",
    sql="""
        DROP TABLE IF EXISTS {{{{ extract_table }}}};
        CREATE TABLE {{{{ extract_table }}}} AS (
            {sql});

        DROP FOREIGN TABLE {{{{ staging_table }}}};
        """,
)
