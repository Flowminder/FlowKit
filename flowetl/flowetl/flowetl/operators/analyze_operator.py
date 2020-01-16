from flowetl.mixins.fixed_sql_with_params_mixin import fixed_sql_operator_with_params

AnalyzeOperator = fixed_sql_operator_with_params(
    class_name="AnalyzeOperator", sql="ANALYZE {{ params.target }};", params=["target"]
)
