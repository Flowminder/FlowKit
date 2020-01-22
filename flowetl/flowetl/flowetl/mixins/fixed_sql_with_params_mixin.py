# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List, Type

from airflow.utils.decorators import apply_defaults


class ParamsMixin:
    @apply_defaults
    def __init__(self, *args, **kwargs) -> None:
        params = kwargs.setdefault("params", {})
        for arg in self.named_params:
            params[arg] = kwargs.pop(arg)
        super().__init__(*args, **kwargs)


def fixed_sql_operator_with_params(
    *, class_name: str, sql: str, params: List[str], is_sensor: bool = False
) -> Type:
    from flowetl.mixins.fixed_sql_mixin import FixedSQLMixin
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin

    if is_sensor:
        from airflow.sensors.sql_sensor import SqlSensor as op_base
    else:
        from airflow.operators.postgres_operator import PostgresOperator as op_base

    return type(
        class_name,
        (TableNameMacrosMixin, ParamsMixin, FixedSQLMixin, op_base),
        dict(fixed_sql=sql, named_params=params),
    )
