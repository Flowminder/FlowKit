# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Proportion of events that are outgoing and
incoming per subscriber.



"""
from ..utilities import EventsTablesUnion
from .metaclasses import SubscriberFeature


class ProportionOutgoing(SubscriberFeature):
    """
    Find the proportion of interactions initiated by a
    given subscriber.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : tuple of float, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    table : str, default 'all'
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    kwargs :
        Passed to flowmachine.EventsTablesUnion

    Notes
    -----
    * `subscriber_identifier` refers only to the subject of the analysis
      so for example subscriber_identifier='imei' will find all the unique
      msisdns that each imei calls. There is currently no way to specify
      the unique number of imei that each subscriber calls for instance.

    Examples
    --------
    >>> ProportionOutgoing("2016-01-01", "2016-01-02",
    table="events.calls")
                   msisdn  proportion_outgoing  proportion_incoming
    0    jWlyLwbGdvKV35Mm                    1                    0
    1    EreGoBpxJOBNl392                  0.7                  0.3
    2    nvKNoAmxMvBW4kJr                  0.4                  0.6
    3    VkzMxYjv7mYn53oK                  0.3                  0.7
    4    BKMy1nYEZpnoEA7G                  0.2                  0.8
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
        if self.subscriber_identifier not in ("msisdn", "imei", "imsi"):
            raise ValueError(
                "Unidentified subscriber id: {}".format(self.subscriber_identifier)
            )

        column_list = [self.subscriber_identifier, "outgoing"]
        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables=self.table,
            columns=column_list,
            subscriber_identifier=self.subscriber_identifier,
            **kwargs
        )
        super().__init__()

    def _make_query(self):

        sql = """

        SELECT
            subscriber,
            sum(outgoing::integer)::float / count(subscriber)::float AS proportion_outgoing,
            1 - (sum(outgoing::integer)::float / count(subscriber)::float) AS proportion_incoming
        FROM ({unioned_query}) AS init_pct
        GROUP BY subscriber
        ORDER BY proportion_outgoing DESC

        """.format(
            unioned_query=self.unioned_query.get_query()
        )

        return sql
