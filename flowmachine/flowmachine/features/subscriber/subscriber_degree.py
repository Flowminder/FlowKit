# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates the total number of events subscribers
have done over a certain time period.


"""
from .metaclasses import SubscriberFeature
from ..utilities.sets import EventTableSubset, EventsTablesUnion


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
    table : str, default 'all'
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
                   msisdn  count
    0    038OVABN11Ak4W5P      2
    1    09NrjaNNvDanD8pk      2
    2    0ayZGYEQrqYlKw6g      2
    3    0DB8zw67E9mZAPK2      2
    4    0Gl95NRLjW2aw8pW      2
    5    0gmvwzMAYbz5We1E      2
    ...

    """

    def __init__(
        self, start, stop, table="all", subscriber_identifier="msisdn", **kwargs
    ):
        """

        """

        self.table = table
        self.start = start
        self.stop = stop
        self.subscriber_identifier = subscriber_identifier
        try:
            self.hours = kwargs["hours"]
        except KeyError:
            self.hours = "ALL"
        column_list = [self.subscriber_identifier, "msisdn_counterpart", "outgoing"]
        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables=self.table,
            columns=column_list,
            subscriber_identifier=self.subscriber_identifier,
            **kwargs
        )
        self._cols = ["subscriber", "degree"]
        super().__init__()

    def _make_query(self):

        sql = """
        SELECT
           subscriber,
            count(*) AS degree FROM
        (SELECT DISTINCT subscriber, msisdn_counterpart
         FROM ({unioned_query}) AS subscriber_degree) AS _
        GROUP BY subscriber
        """.format(
            unioned_query=self.unioned_query.get_query()
        )

        return sql


class SubscriberInDegree(SubscriberDegree):
    """
    Find the total number of unique contacts
    that each subscriber is contacted by.
    """

    def _make_query(self):

        sql = """
        SELECT
            subscriber,
            count(*) AS degree FROM
        (SELECT DISTINCT subscriber, msisdn_counterpart
         FROM ({unioned_query}) AS subscriber_degree
         WHERE subscriber_degree.outgoing = FALSE) AS _
        GROUP BY subscriber
        """.format(
            unioned_query=self.unioned_query.get_query()
        )

        return sql


class SubscriberOutDegree(SubscriberDegree):
    """
    Find the total number of unique contacts
    that each subscriber contacts.
    """

    def _make_query(self):

        sql = """
        SELECT
            subscriber,
            count(*) AS degree FROM
        (SELECT DISTINCT subscriber AS subscriber, msisdn_counterpart
         FROM ({unioned_query}) AS subscriber_degree
         WHERE subscriber_degree.outgoing = TRUE) AS _
        GROUP BY subscriber
        """.format(
            unioned_query=self.unioned_query.get_query()
        )

        return sql
