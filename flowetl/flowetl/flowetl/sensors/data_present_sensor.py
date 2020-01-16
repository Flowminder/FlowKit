from flowetl.mixins.fixed_sql_mixin import fixed_sql_operator

DataPresentSensor = fixed_sql_operator(
    class_name="DataPresentSensor",
    sql="SELECT * FROM {{ staging_table }} LIMIT 1;",
    is_sensor=True,
)
