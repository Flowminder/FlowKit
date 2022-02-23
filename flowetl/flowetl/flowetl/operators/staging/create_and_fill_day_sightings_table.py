# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from airflow.operators.postgres_operator import PostgresOperator


class CreateAndFillDaySightingsTable(PostgresOperator):
    """Reworks the data in the staging table from CRD rows into merged consecutive sighting format (REF: SOMEWHERE)"""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            task_id="CreateAndFillDaySightingsTable",
            sql="create_and_fill_day_sightings_table.sql",
            **kwargs
        )
