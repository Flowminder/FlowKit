# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List, Type


class ParamsMixin:
    def __init__(self, *args, **kwargs) -> None:
        params = kwargs.setdefault("params", {})
        for arg in self.named_params:
            params[arg] = kwargs.pop(arg)
        super().__init__(*args, **kwargs)


def fixed_sql_operator_with_params(
    *, class_name: str, sql: str, params: List[str], is_sensor: bool = False
) -> Type:
    """
    Manufactor a new operator which will run a fixed sql template with some
    values as parameters

    Parameters
    ----------
    class_name : str
        Name of the operator class
    sql : str
        Fixed sql string (will be templated)
    params : list of str
        A list of named parameters the operator should accept at instantiation
    is_sensor : bool, default False
        Set to True if this is a sensor

    Returns
    -------
    Type
        New operator class

    """
    from flowetl.mixins.fixed_sql_mixin import FixedSQLMixin
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin

    if is_sensor:
        from airflow.sensors.sql import SqlSensor as op_base
    else:
        from airflow.providers.common.sql.operators.sql import (
            SQLExecuteQueryOperator as op_base,
        )

    return type(
        class_name,
        (TableNameMacrosMixin, ParamsMixin, FixedSQLMixin, op_base),
        dict(fixed_sql=sql, named_params=params),
    )
