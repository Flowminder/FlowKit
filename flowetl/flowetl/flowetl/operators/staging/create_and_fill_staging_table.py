# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from airflow.operators.postgres_operator import PostgresOperator
from operators.staging.event_columns import event_column_mappings


class CreateAndFillStagingTable(PostgresOperator):
    def __init__(self, *args, event_type_list, **kwargs):

        for event_type in event_type_list:
            if event_type not in event_column_mappings.keys():
                raise KeyError(
                    f"{event_type} not one of {event_column_mappings.keys()}"
                )

        unions = "\nUNION ALL\n".join(
            f"""
            SELECT
                MSISDN,
                IMEI,
                IMSI,
                TAC,
                CELL_ID,
                DATE_TIME,
                EVENT_TYPE
            FROM {event_type}_table_{{{{ds_nodash}}}}
            """
            for event_type in event_type_list
        )

        sql = f"""
        BEGIN;
        CREATE TABLE staging_table_{{{{ds_nodash}}}} AS (
        {unions}
        ORDER BY date_time);
        COMMIT;
        """

        super().__init__(*args, task_id="CreateAndFillStagingTable", sql=sql, **kwargs)
