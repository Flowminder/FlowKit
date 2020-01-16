from flowetl.mixins.wrapping_sql_mixin import wrapped_sql_operator

ExtractFromViewOperator = wrapped_sql_operator(
    class_name="ExtractFromViewOperator",
    sql="""
        DROP TABLE IF EXISTS {{{{ extract_table }}}};
        CREATE TABLE {{{{ extract_table }}}} AS (
            {sql});
        
        DROP VIEW {{{{ staging_table }}}};
        """,
)
