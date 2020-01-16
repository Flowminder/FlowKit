from flowetl.mixins.fixed_sql_with_params_mixin import fixed_sql_operator_with_params

ClusterOperator = fixed_sql_operator_with_params(
    class_name="ClusterOperator",
    sql="""
                DROP INDEX IF EXISTS {{ table_name }}_{{ params.cluster_field }}_idx;
                CREATE INDEX {{ table_name }}_{{ params.cluster_field }}_idx ON {{ extract_table }} ({{ params.cluster_field }});
                CLUSTER {{ extract_table }} USING {{ table_name }}_{{ params.cluster_field }}_idx;
                """,
    params=["cluster_field"],
)
