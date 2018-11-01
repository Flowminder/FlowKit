# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
The total number of events that a subscriber interacts
with a counterpart, and the proportion of events 
that a given contact participates out of the 
subscriber's total event count.

"""
import logging

from .metaclasses import SubscriberFeature
from ..utilities.sets import EventsTablesUnion
from ...core.mixins.graph_mixin import GraphMixin

logger = logging.getLogger("flowmachine").getChild(__name__)


class ContactBalance(GraphMixin, SubscriberFeature):
    """
    This class calculates the total number of events 
    that a subscriber interacts with a counterpart,
    and the proportion of events that a given contact
    participates out of the subscriber's total event count.
    This can be used to calculate a subscriber's contact
    network graph and the respective weighted edges 
    for each contact.

    Parameters
    ----------

    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    table : str, default 'all'
    exclude_self_calls : bool, default True
        Set to false to *include* calls a subscriber made to themself
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    direction : {'in', 'out', 'both'}, default 'both'
        Event direction to include in computation. This
        can be outgoing ('out'), incoming ('in'), or both ('both').


    Examples
    --------

    >>> ContactBalance('2016-01-01', '2016-01-07')
                   msisdn       msisdn_counterpart  events     proportion
    0    038OVABN11Ak4W5P         09NrjaNNvDanD8pk     110           0.54
    1    09NrjaNNvDanD8pk         0ayZGYEQrqYlKw6g      94           0.44
    2    0ayZGYEQrqYlKw6g         0DB8zw67E9mZAPK2      70           0.23
    3    0DB8zw67E9mZAPK2         0DB8zw67E9mZAXFF      20           0.12
    ...
    """

    def __init__(
        self,
        start,
        stop,
        hours="all",
        table="all",
        subscriber_identifier="msisdn",
        direction="both",
        exclude_self_calls=True,
        **kwargs,
    ):
        """
        """
        logger.warn(
            "The the ContactBalance() feature uses CDRs "
            + "IDs for calculating an subscriber's graph. "
            + "If IDs are not generated correctly, this "
            + "feature could yield erroneous results."
        )

        self.table = table
        self.start = start
        self.stop = stop
        self.hours = hours
        self.subscriber_identifier = subscriber_identifier
        self.direction = direction
        self.exclude_self_calls = exclude_self_calls

        if self.direction not in ("both", "in", "out"):
            raise ValueError("Unidentified direction: {}".format(self.direction))

        cols = [self.subscriber_identifier, "id", "msisdn_counterpart", "outgoing"]
        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            columns=cols,
            tables=self.table,
            subscriber_identifier=self.subscriber_identifier,
            **kwargs,
        ).get_query()
        self._cols = ["subscriber", "msisdn_counterpart", "events", "proportion"]
        super().__init__()

    def _make_query(self):

        filters = []
        if self.direction == "in":
            filters.append(
                """
            AND outgoing = FALSE
            """
            )
        elif self.direction == "out":
            filters.append(
                """
            AND outgoing = TRUE
            """
            )
        if self.exclude_self_calls:
            filters.append("subscriber != msisdn_counterpart")
        where_clause = f"WHERE {' AND '.join(filters)} " if len(filters) > 0 else ""

        sql = f"""
        WITH unioned AS (
            SELECT
                *
            FROM ({self.unioned_query}) as U
            {where_clause}
        ),
        total_events AS (
            SELECT
                subscriber,
                count(*) AS events
            FROM (
              SELECT DISTINCT U.subscriber,
                U.id
              FROM unioned AS U) AS _
            GROUP BY subscriber
        )
        SELECT
            U.subscriber,
            U.msisdn_counterpart,
            count(*) as events,
            (count(*)::float / T.events::float) as proportion
        FROM 
        (SELECT DISTINCT U.subscriber,
            U.msisdn_counterpart,
            U.id as id
          FROM unioned as U) AS U
        JOIN total_events AS T
            ON U.subscriber = T.subscriber
        GROUP BY U.subscriber, 
                 U.msisdn_counterpart,
                 T.events
        ORDER BY proportion DESC
        """

        return sql
