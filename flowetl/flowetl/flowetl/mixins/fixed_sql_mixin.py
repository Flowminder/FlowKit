# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Type


class FixedSQLMixin:
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(sql=self.fixed_sql, *args, **kwargs)


def fixed_sql_operator(*, class_name: str, sql: str, is_sensor: bool = False) -> Type:
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin

    if is_sensor:
        from airflow.sensors.sql_sensor import SqlSensor as op_base
    else:
        from airflow.operators.postgres_operator import PostgresOperator as op_base

    return type(
        class_name, (TableNameMacrosMixin, FixedSQLMixin, op_base), dict(fixed_sql=sql),
    )
