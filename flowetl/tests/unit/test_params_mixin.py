# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def test_fixed_sql_with_params():
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    from airflow.sensors.sql_sensor import SqlSensor

    from flowetl.mixins.fixed_sql_with_params_mixin import (
        fixed_sql_operator_with_params,
    )
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin

    new_type = fixed_sql_operator_with_params(
        class_name="DUMMY_TYPE", sql="FIXED_SQL", params=["DUMMY_PARAM"]
    )
    new_instance = new_type(task_id="DUMMY", DUMMY_PARAM="DUMMY_PARAM_VALUE")
    assert new_type.fixed_sql == "FIXED_SQL"
    assert new_type.named_params == ["DUMMY_PARAM"]
    assert isinstance(new_instance, PostgresOperator)
    assert isinstance(new_instance, TableNameMacrosMixin)
    assert type(new_instance).__name__ == "DUMMY_TYPE"
    assert new_instance.params == dict(DUMMY_PARAM="DUMMY_PARAM_VALUE")

    new_type = fixed_sql_operator_with_params(
        class_name="DUMMY_TYPE", sql="FIXED_SQL", params=["DUMMY_PARAM"], is_sensor=True
    )
    new_instance = new_type(
        task_id="DUMMY", DUMMY_PARAM="DUMMY_PARAM_VALUE", conn_id="DUMMY_CONNECTION"
    )
    assert new_type.fixed_sql == "FIXED_SQL"
    assert new_type.named_params == ["DUMMY_PARAM"]
    assert isinstance(new_instance, SqlSensor)
    assert isinstance(new_instance, TableNameMacrosMixin)
    assert type(new_instance).__name__ == "DUMMY_TYPE"
    assert new_instance.params == dict(DUMMY_PARAM="DUMMY_PARAM_VALUE")
