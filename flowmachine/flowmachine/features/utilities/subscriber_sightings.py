# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import structlog
import datetime
from typing import List
from sqlalchemy import select, join, func

from ...core import Query, Table
from ...core.sqlalchemy_utils import (
    get_sqlalchemy_table_definition,
    make_sqlalchemy_column_from_flowmachine_column_description,
    get_sql_string,
)

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class SubscriberSigntings(Query):
    """
    Represents data contained in the subscriber_sightings_fact table.

    Parameters
    ----------
    start : str, default None
        date for the beginning of the time frame, e.g. 2016-01-01 or if None, 
        it will use the min date seen in the `interactions.subscriber_sightings_fact` table.
    stop : str, default None
        date for the end of the time frame, e.g. 2016-01-04 or if None, 
        it will use the max date seen in the `interactions.subscriber_sightings_fact` table.
    subscriber_identifier : {'msisdn', 'imei', 'imsi'}, default 'msisdn'
        The column that identifies the subscriber.

    Examples
    --------
    >>> ss = SubscriberSigntings("2016-01-01", "2016-01-02", subscriber_identifier="imei")
    >>> ss.head()
    """

    def __init__(self, start, stop, *, subscriber_identifier="msisdn"):
        # Set start stop dates
        self.start = start
        self.stop = stop

        # Setup the main subscriber_sightings_fact table
        self.mainTable = Table(
            "interactions.subscriber_sightings_fact", columns=["timestamp", "cell_id"]
        )
        self.sqlalchemy_mainTable = get_sqlalchemy_table_definition(
            self.mainTable.fully_qualified_table_name, engine=Query.connection.engine
        )

        # Chose the identifer from interactions.subscribers table
        # rather than subscriber_sightings_fact - as this will allow
        # us to select the required field.
        self.subTable = Table(
            "interactions.subscriber", columns=[subscriber_identifier]
        )
        self.sqlalchemy_subTable = get_sqlalchemy_table_definition(
            self.subTable.fully_qualified_table_name, engine=Query.connection.engine
        )

        # For referencing - we can get all the column name here - perhaps this isn't needed
        self.columns = self.mainTable.column_names + self.subTable.column_names

        super().__init__()

        # This relies upon the connection object existing
        self._check_dates()

    @property
    def column_names(self) -> List[str]:
        return self.columns

    def _check_dates(self):
        # Get min/max dates from interactions.subscriber_sightings_fact if we are provdied with None
        if self.start is None:
            query = self.connection.engine.execute(
                select([func.min(self.sqlalchemy_mainTable.c.timestamp)])
            )
            self.start = query.fetchall()[0]["min_1"].strftime("%Y-%m-%d")

        if self.stop is None:
            query = self.connection.engine.execute(
                select([func.max(self.sqlalchemy_mainTable.c.timestamp)])
            )
            self.stop = query.fetchall()[0]["max_1"].strftime("%Y-%m-%d")

        # Check if the dates are the same
        if self.start == self.stop:
            raise ValueError("Start and stop are the same.")

    def _make_query_with_sqlalchemy(self):
        # Get the join columns
        subscriber_id = make_sqlalchemy_column_from_flowmachine_column_description(
            self.sqlalchemy_mainTable, "subscriber_id"
        )
        id = make_sqlalchemy_column_from_flowmachine_column_description(
            self.sqlalchemy_subTable, "id"
        )

        # Populate a list of columns that we want to select
        sqlalchemy_columns = [
            make_sqlalchemy_column_from_flowmachine_column_description(
                self.sqlalchemy_mainTable, column_str
            )
            for column_str in self.mainTable.column_names
        ]

        # This may need to be updated if there are more cols on the subTable
        sqlalchemy_columns.append(
            make_sqlalchemy_column_from_flowmachine_column_description(
                self.sqlalchemy_subTable, self.subTable.column_names[0]
            )
        )

        # Finally produce the joined select
        select_stmt = select(sqlalchemy_columns).select_from(
            self.sqlalchemy_mainTable.join(
                self.sqlalchemy_subTable, subscriber_id == id
            )
        )

        return get_sql_string(select_stmt)

    _make_query = _make_query_with_sqlalchemy
