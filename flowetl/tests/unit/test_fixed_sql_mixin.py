# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def test_fixed_sql():
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    from airflow.sensors.sql_sensor import SqlSensor
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin
    from flowetl.mixins.fixed_sql_mixin import fixed_sql_operator

    new_type = fixed_sql_operator(class_name="DUMMY_TYPE", sql="FIXED_SQL")
    new_instance = new_type(task_id="DUMMY")
    assert new_type.fixed_sql == "FIXED_SQL"
    assert isinstance(new_instance, PostgresOperator)
    assert isinstance(new_instance, TableNameMacrosMixin)
    assert type(new_instance).__name__ == "DUMMY_TYPE"
    new_instance.prepare_template()
    assert new_instance.sql == "FIXED_SQL"

    new_type = fixed_sql_operator(
        class_name="DUMMY_TYPE", sql="FIXED_SQL", is_sensor=True
    )
    new_instance = new_type(task_id="DUMMY", conn_id="DUMMY_CONNECTION")
    assert new_type.fixed_sql == "FIXED_SQL"
    assert isinstance(new_instance, SqlSensor)
    assert isinstance(new_instance, TableNameMacrosMixin)
    assert type(new_instance).__name__ == "DUMMY_TYPE"
    new_instance.prepare_template()
    assert new_instance.sql == "FIXED_SQL"
