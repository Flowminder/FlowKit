# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates the total number of events subscribers
have done over a certain time period.


"""
from ..utilities import EventsTablesUnion
from .metaclasses import SubscriberFeature


class TotalSubscriberEvents(SubscriberFeature):
    """
    Class representing the number of calls made over a certain time
    period. This can be subset to either texts or calls and incoming
    outgoing.

    Parameters
    ----------
    start : str
        iso format date range for the beginning of the time frame,
        e.g. 2016-01-01 or 2016-01-01 14:03:01.
    stop : str
        As above.
    direction : {'both','out','in'}, default 'both'
        A string representing whether to include only outgoing events,
        only incoming, or both.
    event_type : str, default 'ALL'
        Restrict the analysis to a certain type of phone event. This
        name must correspond to one table in the events schema, e.g.
        'calls', 'sms' etc. If set to all it will bring in all such
        tables specified in flowmachine.yml.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.

    Notes
    -----
    * A date without a hours and mins will be interpreted as
      midnight of that day, so to get data within a single day
      pass (e.g.) '2016-01-01', '2016-01-02'.

    * Use 24 hr format!

    Examples
    --------
    >>> total_calls = TotalSubscriberEvents('2016-01-01', '2016-01-02', direction='both', types='calls')
       subscriber  total_calls
    0 subscriberA           13
    1 subscriberB            6
    2 subscriberC           18
    ...

    """

    def __init__(
        self,
        start,
        stop,
        direction="both",
        event_type="ALL",
        subscriber_identifier="msisdn",
        **kwargs,
    ):
        """
        """

        self.start = start
        self.stop = stop
        self.direction = direction
        self.event_type = event_type
        self.subscriber_identifier = subscriber_identifier

        if self.direction not in ["both", "out", "in"]:
            raise ValueError("Unrecognised direction {}".format(self.direction))

        if self.event_type == "ALL":
            tables = [f"events.{t}" for t in self.connection.subscriber_tables]
        else:
            tables = [f"events.{self.event_type}"]

        cols = [self.subscriber_identifier, "outgoing"]
        self.unioned = EventsTablesUnion(
            self.start,
            self.stop,
            tables=tables,
            columns=cols,
            subscriber_identifier=self.subscriber_identifier,
            **kwargs,
        )

        super().__init__()

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """
        if self.direction == "both":
            clause = ""
        elif self.direction == "out":
            clause = "WHERE outgoing"
        elif self.direction == "in":
            clause = "WHERE NOT outgoing"
        else:
            raise ValueError("Unrecognised direction {}".format(self.direction))

        sql = """
        SELECT
            unioned.subscriber,
            count(*) AS total
        FROM
            ({unioned}) unioned
        {clause}
        GROUP BY
            unioned.subscriber
        ORDER BY
            total DESC
        """.format(
            unioned=self.unioned.get_query(), clause=clause
        )

        return sql
