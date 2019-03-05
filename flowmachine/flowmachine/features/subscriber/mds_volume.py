# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Class for calculating MDS volume statistics.
"""

import warnings

from ..utilities.sets import EventsTablesUnion
from .metaclasses import SubscriberFeature

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}


class MDSVolume(SubscriberFeature):
    """
    This class calculates statistics associated with MDS volume usage.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    volume: {"total", "upload", "download"}, default "total"
        The type of volume.
    statistic : {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to sum, aggregation statistic over the durations.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    tables : str or list of strings, default 'events.mds'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables

    Examples
    --------

    >>> s = MDSVolume("2016-01-01", "2016-01-08")
    >>> s.get_dataframe()

          subscriber  volume_sum
    37J9rKydzJ0mvo0z     8755.36
    PWlZ8Q5zjYkGJzXg    11383.83
    6zm4b7veY2ljEQ95     7481.94
    w1N59Jo07vbgrKae    12301.75
    QoD6rWNN03WPbEBg    10347.43
                 ...         ...
    """

    def __init__(
        self,
        start,
        stop,
        volume="total",
        statistic="sum",
        *,
        subscriber_identifier="msisdn",
        hours="all",
        subscriber_subset=None,
        tables="events.mds",
    ):
        self.start = start
        self.stop = stop
        self.subscriber_identifier = subscriber_identifier
        self.hours = hours
        self.volume = volume
        self.statistic = statistic.lower()
        self.tables = tables

        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        if volume not in {"total", "upload", "download"}:
            raise ValueError("{} is not a valid volume.".format(self.volume))

        column_list = [self.subscriber_identifier, f"volume_{volume}"]

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables=self.tables,
            columns=column_list,
            hours=hours,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )

        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", f"value"]

    def _make_query(self):

        return f"""
        SELECT subscriber, {self.statistic}(volume_{self.volume}) AS value
        FROM ({self.unioned_query.get_query()}) U
        GROUP BY subscriber
        """
