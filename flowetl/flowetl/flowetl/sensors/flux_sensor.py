from flowetl.mixins.fixed_sql_with_params_mixin import fixed_sql_operator_with_params

FluxSensor = fixed_sql_operator_with_params(
    class_name="FluxSensor",
    sql="""
                CREATE TEMPORARY TABLE {{ staging_table_name }}_count AS
                SELECT count(*) FROM {{ staging_table }};
                SELECT pg_sleep({{ params.flux_check_interval }});
                SELECT 1 WHERE (SELECT * FROM {{ staging_table_name }}_count) = (SELECT count(*) FROM {{ staging_table }});
                """,
    params=["flux_check_interval"],
    is_sensor=True,
)
