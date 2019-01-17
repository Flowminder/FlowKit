# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Number of call days over a certain period
of time that a given subscriber makes. This feature
represent the number of days that a subscriber
is connected to a given tower within a
specified time period.
"""
from typing import List

from flowmachine.utils.utils import get_columns_for_level
from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import subscriber_locations


class CallDays(SubscriberFeature):
    """
    Class representing the number of call days over a certain
    period of time.  Call days represent the number of days that a
    subscriber was connected to a tower in the given period of time.

    Parameters
    ----------
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    See Also
    --------
    flowmachine.features.subscriber_locations
    """

    def __init__(
        self,
        start,
        stop,
        *,
        level="cell",
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        ignore_nulls=True,
        column_name=None,
        subscriber_subset=None,
        polygon_table=None,
        size=None,
        radius=None,
    ):
        """


        """
        # the call days class just need the distinct subscriber-location
        # per day
        self.ul = subscriber_locations(
            start=start,
            stop=stop,
            level=level,
            hours=hours,
            table=table,
            subscriber_identifier=subscriber_identifier,
            ignore_nulls=ignore_nulls,
            column_name=column_name,
            subscriber_subset=subscriber_subset,
            polygon_table=polygon_table,
            size=size,
            radius=radius,
        )
        self.level = self.ul.level
        self.column_name = self.ul.column_name

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return (
            ["subscriber"]
            + get_columns_for_level(self.level, self.column_name)
            + ["calldays"]
        )

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        Returns a sorted calldays table.
        """
        relevant_columns = ", ".join(
            get_columns_for_level(self.level, self.column_name)
        )

        sql = f"""
        SELECT * FROM (
            SELECT
                connections.subscriber,
                {relevant_columns},
                COUNT(*) AS calldays
            FROM (
                SELECT DISTINCT locations.subscriber, {relevant_columns}, locations.time::date
                FROM ({self.ul.get_query()}) AS locations
            ) AS connections
            GROUP BY connections.subscriber, {relevant_columns}
        ) calldays
        ORDER BY calldays.subscriber ASC, calldays.calldays DESC
        """

        return sql

    def _get_timestamp(self, date):
        """
        Gets the POSTGRES `timestamptz` representation of a given date. This
        function ensures that all dates passed to the function will have a
        uniform format.

        Parameters
        ----------
        date : str
            A string representing a date.
        """
        con = self.connection.engine
        with con.begin():
            date = con.execute("SELECT '{}'::timestamptz".format(date))
            date = date.fetchone()[0]
        date = date.strftime("%Y%m%d%H%M%S")
        return date
