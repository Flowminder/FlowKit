# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def test_wrapped_sql():
    from airflow.providers.postgres.operators.postgres import PostgresOperator
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
