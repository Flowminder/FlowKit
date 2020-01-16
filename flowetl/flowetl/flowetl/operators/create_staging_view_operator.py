from flowetl.mixins.wrapping_sql_mixin import wrapped_sql_operator

CreateStagingViewOperator = wrapped_sql_operator(
    class_name="CreateStagingViewOperator",
    sql="CREATE OR REPLACE VIEW {{{{ staging_table }}}} AS {sql}",
)
