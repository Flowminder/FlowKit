from flowetl.mixins.wrapping_sql_mixin import wrapped_sql_operator

QACheckOperator = wrapped_sql_operator(
    sql="""INSERT INTO etl.post_etl_queries(cdr_date, cdr_type, type_of_query_or_check, outcome, timestamp) 
            VALUES
        (date '{{{{ ds }}}}', '{{{{ params.cdr_type }}}}', '{{{{ task.task_id }}}}', ({sql}), NOW())
    """,
    class_name="QACheckOperator",
)
