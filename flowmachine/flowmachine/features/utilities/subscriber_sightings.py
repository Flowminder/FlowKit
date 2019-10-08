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
    Represents data contained in the subscriber_sightings_fact table.

    Parameters
    ----------
    start : str, default None
        date for the beginning of the time frame, e.g. 2016-01-01 or if None, 
        it will use the min date seen in the `interactions.subscriber_sightings_fact` table.
    stop : str, default None
        date for the end of the time frame, e.g. 2016-01-04 or if None, 
        it will use the max date seen in the `interactions.subscriber_sightings_fact` table.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    subscriber_identifier : {'msisdn', 'imei', 'imsi'}, default 'msisdn'
        The column that identifies the subscriber.

    Examples
    --------
    >>> ss = SubscriberSightings("2016-01-01", "2016-01-02", subscriber_identifier="imei")
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
        self.subscriber_subsetter = make_subscriber_subsetter(subscriber_subset)
        self.subscriber_identifier = subscriber_identifier.lower()

        # Setup the main subscriber_sightings_fact & subscriber tables
        self.sqlalchemy_mainTable = get_sqlalchemy_table_definition(
            "interactions.subscriber_sightings_fact", engine=Query.connection.engine
        )
        self.sqlalchemy_subTable = get_sqlalchemy_table_definition(
            "interactions.subscriber", engine=Query.connection.engine
        )

        self.columns = [
            "timestamp AS datetime",
            "cell_id AS location_id",
            f"{self.subscriber_identifier} AS subscriber",
        ]

        super().__init__()

        # This relies upon the connection object existing
        self._check_dates()

    @property
    def column_names(self) -> List[str]:
        return [c.split(" AS ")[-1] for c in self.columns]

    def _check_dates(self):
        table = get_sqlalchemy_table_definition(
            "interactions.date_dim", engine=Query.connection.engine
        )

        # First, if there are dates, then we should convert them to integers
        for param in ("start", "stop"):
            val = getattr(self, param)
            if val is not None:
                query = self.connection.engine.execute(
                    select([table.c.date_sk]).where(table.c.date == val)
                )
                row = query.first()
                setattr(self, param, (row["date_sk"] if row is not None else None))

        # If we still have None, then we should get min/max dates from interactions.date_dim
        # Process self.start
        if self.start is None:
            query = self.connection.engine.execute(
                select([table.c.date_sk]).where(
                    table.c.date == select([func.min(table.c.date)])
                )
            )
            row = query.first()
            self.start = row["date_sk"] if row is not None else None

        # Then self.stop
        if self.stop is None:
            query = self.connection.engine.execute(
                select([table.c.date_sk]).where(
                    table.c.date == select([func.max(table.c.date)])
                )
            )
            row = query.first()
            self.stop = row["date_sk"] if row is not None else None

        # Check if the dates are the same
        if self.start == self.stop:
            raise ValueError("Start and stop are the same.")

    def _make_query_with_sqlalchemy(self):
        # As we are adding from multiple tables, we need to add one by one - here we need
        # to substitute the fieldnames for referencing later according to self.column_names
        sqlalchemy_columns = [
            make_sqlalchemy_column_from_flowmachine_column_description(
                self.sqlalchemy_mainTable, self.columns[0]
            ),
            make_sqlalchemy_column_from_flowmachine_column_description(
                self.sqlalchemy_mainTable, self.columns[1]
            ),
            make_sqlalchemy_column_from_flowmachine_column_description(
                self.sqlalchemy_subTable, self.columns[2]
            ),
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
                self.sqlalchemy_mainTable.c.date_sk >= self.start
            )

        # Add the stop date - this will need hours added to it at some point.
        if self.stop is not None:
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
