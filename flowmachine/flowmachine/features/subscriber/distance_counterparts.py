# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculate metrics related with distance between caller and her/his counterparts.
"""
from typing import List

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}

from ..utilities import EventsTablesUnion
from .metaclasses import SubscriberFeature
from ..spatial.distance_matrix import DistanceMatrix


class DistanceCounterparts(SubscriberFeature):
    """
    This class returns metrics related with the distance between event
    initiator and her/his counterparts.

    It assumes that the ID column uniquely identifies the event initiator and
    their counterparts' event. Choose only tables for which this assumption is
    true. In some cases, asynchronous communication like SMS might not be
    tagged with an ID that allows one to recover the counterpart event.

    Distances are measured in km.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    tables: str, default 'all'.
        The table must have a `msisdn_counterpart` column.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    statistic :  {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'avg'
    exclude_self_calls : bool, default True
        Set to false to *include* calls a subscriber made to themself
        Defaults to sum, aggregation statistic over the durations.


    Examples
    --------

    >>> s = DistanceCounterparts("2016-01-01", "2016-01-07", statistic="avg")
    >>> s.get_dataframe()

              subscriber    distance_avg
        038OVABN11Ak4W5P      272.167815
        09NrjaNNvDanD8pk      241.290233
        0ayZGYEQrqYlKw6g      218.161568
        0DB8zw67E9mZAPK2      228.235324
        0Gl95NRLjW2aw8pW      189.008980
                     ...             ...

    """

    def __init__(
        self,
        start,
        stop,
        statistic="avg",
        *,
        hours="all",
        tables="all",
        direction="both",
        subscriber_subset=None,
        exclude_self_calls=True,
    ):
        self.tables = tables
        self.start = start
        self.stop = stop
        self.hours = hours
        self.direction = direction
        self.exclude_self_calls = exclude_self_calls

        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        column_list = ["msisdn", "msisdn_counterpart", "id", "location_id", "outgoing"]
        self.tables = tables

        # EventsTablesUnion will only subset on the subscriber identifier,
        # which means that we need to query for a unioned table twice. That has
        # a considerable negative impact on execution time.
        self.unioned_from_query = EventsTablesUnion(
            self.start,
            self.stop,
            columns=column_list,
            tables=self.tables,
            subscriber_identifier="msisdn",
            hours=hours,
            subscriber_subset=subscriber_subset,
        ).get_query()

        self.unioned_to_query = EventsTablesUnion(
            self.start,
            self.stop,
            columns=column_list,
            tables=self.tables,
            subscriber_identifier="msisdn_counterpart",
            hours=hours,
            subscriber_subset=subscriber_subset,
        ).get_query()

        self.distance_matrix = DistanceMatrix()

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", f"distance_{self.statistic}"]

    def _make_query(self):

        filters = []
        if self.direction != "both":
            filters.append(
                f"A.outgoing = {'TRUE' if self.direction == 'out' else 'FALSE'}"
            )
        if self.exclude_self_calls:
            filters.append("A.subscriber != A.msisdn_counterpart")
        on_filters = f"AND {' AND '.join(filters)} " if len(filters) > 0 else ""

        sql = f"""
        SELECT
            U.subscriber AS subscriber,
            {self.statistic}(D.distance) AS distance_{self.statistic}
        FROM
            (
                SELECT A.subscriber, A.location_id AS location_id_from, B.location_id AS location_id_to FROM
                ({self.unioned_from_query}) AS A
                JOIN ({self.unioned_to_query}) AS B
                ON A.id = B.id AND A.outgoing != B.outgoing {on_filters}
            ) U
        JOIN
            ({self.distance_matrix.get_query()}) D
        ON U.location_id_from = D.location_id_from AND U.location_id_to = D.location_id_to
        GROUP BY U.subscriber
        """

        return sql
