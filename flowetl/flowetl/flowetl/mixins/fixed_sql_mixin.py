# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Type


class FixedSQLMixin:
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(sql=self.fixed_sql, *args, **kwargs)


def fixed_sql_operator(*, class_name: str, sql: str, is_sensor: bool = False) -> Type:
    """
    Manufactor a new operator which will run a fixed sql template.

    Parameters
    ----------
    class_name : str
        Name of the operator class
    sql : str
        Fixed sql string (will be templated)
    is_sensor : bool, default False
        Set to True if this is a sensor

    Returns
    -------
    Type
        New operator class

    """
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin

    if is_sensor:
        from airflow.sensors.sql import SqlSensor as op_base
    else:
        from airflow.providers.postgres.operators.postgres import (
            PostgresOperator as op_base,
        )

    return type(
        class_name,
        (TableNameMacrosMixin, FixedSQLMixin, op_base),
        dict(fixed_sql=sql),
    )
