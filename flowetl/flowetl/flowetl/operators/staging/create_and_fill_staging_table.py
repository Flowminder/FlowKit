# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import List
from airflow.operators.postgres_operator import PostgresOperator
from flowetl.operators.staging.event_columns import event_column_mappings


class CreateAndFillStagingTable(PostgresOperator):
    """
    Creates a new table containing only identifiers (MSISDN, IMEI, IMSI, TAC), cell_id, time of contact
    and contact event type (defined in sql/event_type_enums.csv) from foreign tables mounted by operators
    produced by mount_event_operator_factory.

    Parameters
    ----------
    event_type_list:List[str]
        A list of event types; any or all of 'call','sms','location','mds','topup'
    """

    def __init__(self, *args, event_type_list: List[str], **kwargs):

        if notin := set(event_type_list).difference(set(event_column_mappings.keys())):
            # TODO: testme
            raise KeyError(f"{notin} not found in {event_column_mappings.keys()}")

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
