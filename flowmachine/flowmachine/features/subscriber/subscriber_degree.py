# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates the total number of events subscribers
have done over a certain time period.


"""
from typing import List

from .metaclasses import SubscriberFeature
from ..utilities import EventsTablesUnion


class SubscriberDegree(SubscriberFeature):
    """
    Find the total number of unique contacts
    that each subscriber interacts with.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    tables : str, default 'all'
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    kwargs
        Passed to flowmachine.EventTableUnion

    Notes
    -----

    `subscriber_identifier` refers only to the subject of the analysis
    so for example subscriber_identifier='imei' will find all the unique
    msisdns that each imei calls. There is currently no way to specify
    the unique number of imei that each subscriber calls for instance.

    Examples
    --------

    >>> SubscriberDegree('2016-01-01', '2016-01-01')
                   msisdn  value
    0    038OVABN11Ak4W5P      2
    1    09NrjaNNvDanD8pk      2
    2    0ayZGYEQrqYlKw6g      2
    3    0DB8zw67E9mZAPK2      2
    4    0Gl95NRLjW2aw8pW      2
    5    0gmvwzMAYbz5We1E      2
    ...

    """

    def __init__(
        self,
        start,
        stop,
        *,
        hours="all",
        tables="all",
        subscriber_identifier="msisdn",
        direction="both",
        exclude_self_calls=True,
        subscriber_subset=None,
    ):
        self.start = start
        self.stop = stop
        self.hours = hours
        self.direction = direction
        self.subscriber_identifier = subscriber_identifier
        self.exclude_self_calls = exclude_self_calls
        self.tables = tables

        if self.direction in {"both"}:
            column_list = [self.subscriber_identifier, "msisdn_counterpart"]
        elif self.direction in {"in", "out"}:
            column_list = [self.subscriber_identifier, "msisdn_counterpart", "outgoing"]
        else:
            raise ValueError("{} is not a valid direction.".format(self.direction))

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            hours=self.hours,
            tables=self.tables,
            columns=column_list,
            subscriber_identifier=self.subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )
        self._cols = ["subscriber", "degree"]
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):

        filters = []
        if self.direction != "both":
            filters.append(
                f"outgoing = {'TRUE' if self.direction == 'out' else 'FALSE'}"
            )
        if self.exclude_self_calls:
            filters.append("subscriber != msisdn_counterpart")
        where_clause = f"WHERE {' AND '.join(filters)} " if len(filters) > 0 else ""

        sql = f"""
        SELECT
           subscriber,
           COUNT(*) AS value
        FROM (
            SELECT DISTINCT subscriber, msisdn_counterpart
            FROM ({self.unioned_query.get_query()}) AS U
            {where_clause}
        ) AS U
        GROUP BY subscriber
        """

        return sql
