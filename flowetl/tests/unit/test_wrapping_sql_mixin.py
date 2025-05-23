# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def test_wrapped_sql(mock_basic_dag):
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin
    from flowetl.mixins.wrapping_sql_mixin import wrapped_sql_operator

    new_type = wrapped_sql_operator(class_name="DUMMY_TYPE", sql="WRAPS({sql})")
    new_instance = new_type(sql="WRAPPED", task_id="DUMMY", dag=mock_basic_dag)
    assert new_type.wrapper_sql == "WRAPS({sql})"
    assert isinstance(new_instance, SQLExecuteQueryOperator)
    assert isinstance(new_instance, TableNameMacrosMixin)
    assert type(new_instance).__name__ == "DUMMY_TYPE"
    new_instance.prepare_template()
    assert new_instance.sql == "WRAPS(WRAPPED)"
