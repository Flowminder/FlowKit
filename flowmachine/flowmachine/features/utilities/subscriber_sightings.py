# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import structlog
import datetime
import pandas as pd
from typing import List
from sqlalchemy import select, join, func

from ...core import Query
from ...core.sqlalchemy_utils import (
    get_sqlalchemy_table_definition,
    make_sqlalchemy_column_from_flowmachine_column_description,
    get_sql_string,
)
from flowmachine.core.subscriber_subsetter import make_subscriber_subsetter

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class SubscriberSightings(Query):
    """
    Represents data contained in the subscriber_sightings table.

    Parameters
    ----------
    start : str, default None
        date for the beginning of the time frame, e.g. 2016-01-01 or if None, 
        it will use the min date seen in the `interactions.subscriber_sightings` table.
    stop : str, default None
        date for the end of the time frame, e.g. 2016-01-04 or if None, 
        it will use the max date seen in the `interactions.subscriber_sightings` table.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    subscriber_identifier : {'msisdn', 'imei', 'imsi'}, default 'msisdn'
        The column that identifies the subscriber.

    Examples
    --------
    >>> ss = SubscriberSightings("2016-01-01 13:00:00", "2016-01-02 18:00:00", subscriber_identifier="imei")
    >>> ss.head()
    """

    def __init__(
        self,
        start,
        stop,
        *,
        subscriber_subset=None,
        subscriber_identifier="msisdn",
        columns=[],
        tables=None,
        hours="all",
    ):
        # Note: the columns, tables and hours arguments are only
        # passed for completeness, they will need to be added in.

        # Set start stop dates, subscriber_subsetter & subscriber_identifier
        self.start = start
        self.stop = stop
        self.start_hour = None
        self.stop_hour = None
        self.subscriber_subsetter = make_subscriber_subsetter(subscriber_subset)
        self.subscriber_identifier = subscriber_identifier.lower()

        # Setup the main subscriber_sightings & subscriber tables
        self.sqlalchemy_mainTable = get_sqlalchemy_table_definition(
            "interactions.subscriber_sightings", engine=Query.connection.engine
        )
        self.sqlalchemy_subTable = get_sqlalchemy_table_definition(
            "interactions.subscriber", engine=Query.connection.engine
        )

        self.columns = [
            "timestamp AS datetime",
            "cell_id AS location_id",
            "subscriber_id AS subscriber",
            f"{self.subscriber_identifier} AS subscriber_identifier",
        ]

        super().__init__()

        # This relies upon the connection object existing
        self._check_dates()

    @property
    def column_names(self) -> List[str]:
        return [c.split(" AS ")[-1] for c in self.columns]

    @property
    def singleday(self) -> bool:
        return self.start == self.stop

    def _check_dates(self):
        if self.start is not None:
            self.start_hour = self._resolveDateOrTime(time=self.start, min=True)
            self.start = self._resolveDateOrTime(date=self.start, min=True)

        # We need to only process the stop value for time/date if it's set
        if self.stop is not None:
            self.stop_hour = self._resolveDateOrTime(time=self.stop, max=True)
            self.stop = self._resolveDateOrTime(date=self.stop, max=True)
        else:
            self.stop = self.start

        if self.start is None and self.stop is None:
            raise ValueError("Please set valid dates that exist within the data set")

    def _resolveDateOrTime(self, date=None, time=None, min=False, max=False):
        if (date == None and time == None and min == False and max == False) or (
            min == True and max == True
        ):
            return None

        table = get_sqlalchemy_table_definition(
            "interactions." + ("date_dim" if date is not None else "time_dimension"),
            engine=Query.connection.engine,
        )

        try:
            ts = pd.Timestamp((date if date is not None else time))
        except ValueError:
            raise ValueError("Please use a valid date or time string")

        field = "date" if date is not None else "hour"
        sel = "date_sk" if date is not None else "time_sk"
        comp = ts.strftime("%Y-%m-%d") if date is not None else ts.hour

        # Test if we can get a value
        query = self.connection.engine.execute(
            select([table.c[sel]]).where(table.c[field] == comp)
        )
        row = query.first()
        if row is not None:
            return row[sel]

        # Return None if we're not being asked for a min or max
        if min == False and max == False:
            return None

        # Find min/max if this is a required outcome
        if min == True and date is not None:
            comp = select([func.min(table.c[field])])
        if max == True and date is not None:
            comp = select([func.max(table.c[field])])

        query = self.connection.engine.execute(
            select([table.c[sel]]).where(table.c[field] == comp)
        )
        row = query.first()

        return row[sel] if row is not None else None

    def _make_query_with_sqlalchemy(self):
        # As we are adding from multiple tables, we need to add one by one - here we need
        # to substitute the fieldnames for referencing later according to self.column_names
        sqlalchemy_columns = [
            make_sqlalchemy_column_from_flowmachine_column_description(
                (self.sqlalchemy_mainTable if i < 3 else self.sqlalchemy_subTable),
                self.columns[i],
            )
            for i, c in enumerate(self.columns)
        ]

        # Finally produce the joined select
        select_stmt = select(sqlalchemy_columns).select_from(
            self.sqlalchemy_mainTable.join(
                self.sqlalchemy_subTable,
                self.sqlalchemy_mainTable.c.subscriber_id
                == self.sqlalchemy_subTable.c.id,
            )
        )

        # Add the start date - this will need hours added to it at some point.
        if self.start is not None:
            select_stmt = select_stmt.where(
                (
                    self.sqlalchemy_mainTable.c.date_sk == self.start
                    if self.singleday == True
                    else self.sqlalchemy_mainTable.c.date_sk >= self.start
                )
            )

        # Add the stop date - this will need hours added to it at some point.
        if self.stop is not None and self.singleday == False:
            select_stmt = select_stmt.where(
                self.sqlalchemy_mainTable.c.date_sk < self.stop
            )

        # Added the subscriber_subsetter
        if self.subscriber_subsetter is not None:
            select_stmt = self.subscriber_subsetter.apply_subset_if_needed(
                select_stmt, subscriber_identifier=self.subscriber_identifier
            )

        return get_sql_string(select_stmt)

    _make_query = _make_query_with_sqlalchemy
