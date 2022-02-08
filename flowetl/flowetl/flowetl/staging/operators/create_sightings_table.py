# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from airflow.operators.postgres_operator import PostgresOperator


class CreateSightingsTable(PostgresOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            task_id="CreateSightingsTable",
            sql="operators/sql/create_sightings_table.sql",
            **kwargs
        )
