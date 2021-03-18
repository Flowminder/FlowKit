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
from typing import List, Union, Optional, Tuple

from flowmachine.core.mixins import GraphMixin
from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.direction_enum import Direction
from flowmachine.utils import make_where, standardise_date


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
    tables : str, default 'all'
    exclude_self_calls : bool, default True
        Set to false to *include* calls a subscriber made to themself
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    direction : {'in', 'out', 'both'} or Direction, default Direction.BOTH
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
        *,
        hours: Optional[Tuple[int, int]] = None,
        tables="all",
        subscriber_identifier="msisdn",
        direction: Union[str, Direction] = Direction.BOTH,
        exclude_self_calls=True,
        subscriber_subset=None,
    ):
        self.tables = tables
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.hours = hours
        self.direction = Direction(direction)
        self.subscriber_identifier = subscriber_identifier
        self.exclude_self_calls = exclude_self_calls
        self.tables = tables

        column_list = [
            self.subscriber_identifier,
            "msisdn_counterpart",
            *self.direction.required_columns,
        ]

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            columns=column_list,
            tables=self.tables,
            subscriber_identifier=self.subscriber_identifier,
            hours=hours,
            subscriber_subset=subscriber_subset,
        )
        self._cols = ["subscriber", "msisdn_counterpart", "events", "proportion"]
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "msisdn_counterpart", "events", "proportion"]

    def _make_query(self):

        filters = [self.direction.get_filter_clause()]
        if (self.subscriber_identifier in {"msisdn"}) and (self.exclude_self_calls):
            filters.append("subscriber != msisdn_counterpart")
        where_clause = make_where(filters)

        sql = f"""
        WITH unioned AS (
            SELECT
                *
            FROM ({self.unioned_query.get_query()}) as U
            {where_clause}
        ),
        total_events AS (
            SELECT
                subscriber,
                count(*) AS events
            FROM unioned
            GROUP BY subscriber
        )
        SELECT
            U.subscriber,
            U.msisdn_counterpart,
            count(*) as events,
            (count(*)::float / T.events::float) as proportion
        FROM
        (SELECT U.subscriber,
            U.msisdn_counterpart
          FROM unioned as U) AS U
        JOIN total_events AS T
            ON U.subscriber = T.subscriber
        GROUP BY U.subscriber,
                 U.msisdn_counterpart,
                 T.events
        ORDER BY proportion DESC
        """

        return sql

    def counterparts_subset(self, include_subscribers=False):
        """
        Returns the subset of counterparts. In some cases, we are interested in
        obtaining information about the subset of subscribers contacts.

        This method also allows one to get the subset of counterparts together
        with subscribers by turning the `include_subscribers` flag to `True`.

        Parameters
        ----------
        include_subscribers: bool, default True
            Wether to include the list of subscribers in the subset as well.
        """

        return _ContactBalanceSubset(
            contact_balance=self, include_subscribers=include_subscribers
        )


class _ContactBalanceSubset(SubscriberFeature):
    """
    This internal class returns the subset of counterparts. In some cases, we
    are interested in obtaining information about the subset of subscribers
    contacts.

    This method also allows one to get the subset of counterparts together with
    subscribers by turning the `include_subscribers` flag to `True`.

    Parameters
    ----------
    include_subscribers: bool, default False
        Wether to include the list of subscribers in the subset as well.
    """

    def __init__(self, contact_balance, include_subscribers=False):

        self.contact_balance_query = contact_balance
        self.include_subscribers = include_subscribers

        if (
            self.contact_balance_query.subscriber_identifier in {"imei"}
            and self.include_subscribers
        ):
            raise ValueError(
                """
                The counterparts are always identified are identified via the
                msisdn while the subscribers are being identified via the imei.
                Therefore, it is not possible to extract the counterpart subset
                and merge with the subscriber subset. Performing otherwise
                would be inconsistent.
            """
            )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber"]

    def _make_query(self):

        include_subscriber_clause = ""
        if (
            self.include_subscribers
            and self.contact_balance_query.subscriber_identifier in {"msisdn"}
        ):
            include_subscriber_clause = f"""
            UNION SELECT DISTINCT subscriber FROM ({self.contact_balance_query.get_query()}) C
            """

        return f"""
        SELECT DISTINCT msisdn_counterpart AS subscriber
        FROM ({self.contact_balance_query.get_query()}) C
        {include_subscriber_clause}
        """
