# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from airflow.operators.sensors import SqlSensor
from airflow.utils.decorators import apply_defaults
from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin


class FileFluxSensor(TableNameMacrosMixin, SqlSensor):
    @apply_defaults
    def __init__(
        self, *, conn_id: str, flux_check_interval: int, filename: str, **kwargs
    ) -> None:
        self.filename = filename
        self.flux_check_interval = flux_check_interval
        sql = f"""
                CREATE TEMPORARY TABLE {{{{ staging_table_name }}}}_mod_date AS
                SELECT (pg_stat_file('{filename}')).modification;
                SELECT pg_sleep({ flux_check_interval });
                SELECT 1 WHERE (SELECT * FROM {{{{ staging_table_name }}}}_mod_date) = ((pg_stat_file('{filename}')).modification);
                """
        super().__init__(conn_id=conn_id, sql=sql, **kwargs)
