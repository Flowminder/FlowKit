def test_wrapped_sql():
    from airflow.operators.postgres_operator import PostgresOperator
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin
    from flowetl.mixins.wrapping_sql_mixin import wrapped_sql_operator

    new_type = wrapped_sql_operator(class_name="DUMMY_TYPE", sql="WRAPS({sql})")
    new_instance = new_type(sql="WRAPPED", task_id="DUMMY")
    assert new_type.wrapper_sql == "WRAPS({sql})"
    assert isinstance(new_instance, PostgresOperator)
    assert isinstance(new_instance, TableNameMacrosMixin)
    assert type(new_instance).__name__ == "DUMMY_TYPE"
    new_instance.prepare_template()
    assert new_instance.sql == "WRAPS(WRAPPED)"
