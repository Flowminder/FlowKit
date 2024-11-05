# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Class for calculating MDS volume statistics.
"""
from typing import Optional, Tuple

from ..utilities.sets import EventsTablesUnion
from .metaclasses import SubscriberFeature
from flowmachine.utils import standardise_date
from ...core.statistic_types import Statistic


class MDSVolume(SubscriberFeature):
    """
    This class calculates statistics associated with MDS (mobile data session) volume usage.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    volume: {"total", "upload", "download"}, default "total"
        The type of volume.
    statistic : Statistic, default Statistic.SUM
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

    Examples
    --------

    >>> s = MDSVolume("2016-01-01", "2016-01-08")
    >>> s.get_dataframe()

          subscriber       value
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
        statistic: Statistic = Statistic.SUM,
        *,
        subscriber_identifier="msisdn",
        hours: Optional[Tuple[int, int]] = None,
        subscriber_subset=None,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.subscriber_identifier = subscriber_identifier
        self.hours = hours
        self.volume = volume
        self.statistic = Statistic(statistic.lower())
        self.tables = "events.mds"

        if self.volume not in {"total", "upload", "download"}:
            raise ValueError(f"{self.volume} is not a valid volume.")

        column_list = [self.subscriber_identifier, f"volume_{self.volume}"]

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
        return ["subscriber", "value"]

    def _make_query(self):
        return f"""
        SELECT subscriber, {self.statistic:volume_{self.volume}} AS value
        FROM ({self.unioned_query.get_query()}) U
        GROUP BY subscriber
        """
