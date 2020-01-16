# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from airflow.operators.postgres_operator import PostgresOperator
from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin


class AnalyzeOperator(TableNameMacrosMixin, PostgresOperator):
    def __init__(self, *, target, **kwargs) -> None:
        super().__init__(
            sql=f"ANALYZE {target};", **kwargs
        )  # Need an f-string to let us use templating with the target
