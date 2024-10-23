# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from typing import Union, Optional, Tuple

from flowmachine.core import location_joined_query
from flowmachine.core.spatial_unit import AnySpatialUnit, make_spatial_unit
from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.direction_enum import Direction
from flowmachine.utils import make_where, standardise_date
from flowmachine.core.statistic_types import Statistic


class PerLocationEventStats(SubscriberFeature):
    """
    This class returns the statistics of event count per location per
    subscriber within the period, optionally limited to only incoming or
    outgoing events. For instance, it calculates the average number of events
    per cell per subscriber.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    statistic : Statistic, default Statistic.AVG
        Defaults to avg, aggregation statistic over the durations.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    direction : {'in', 'out', 'both'} or Direction, default Direction.BOTH
        Whether to consider calls made, received, or both. Defaults to 'both'.
    tables : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables
    spatial_unit : flowmachine.core.spatial_unit.*SpatialUnit, default cell
        Spatial unit to which subscriber locations will be mapped. See the
        docstring of make_spatial_unit for more information.

    Examples
    --------

    >>> s = PerLocationEventStats("2016-01-01", "2016-01-07")
    >>> s.get_dataframe()

          subscriber      value
    OemQ7q2DLZMWnwzB   1.388889
    By4j6PKdB4NGMpxr   1.421053
    L4V537alj321eWz6   1.130435
    4pQo67v0PWyLdYKO   1.400000
    8br1gO32xWXxjY0R   1.100000
                 ...        ...

    """

    def __init__(
        self,
        start,
        stop,
        statistic: Statistic = Statistic.AVG,
        *,
        spatial_unit: AnySpatialUnit = make_spatial_unit("cell"),
        hours: Optional[Tuple[int, int]] = None,
        tables="all",
        subscriber_identifier="msisdn",
        direction: Union[str, Direction] = Direction.BOTH,
        subscriber_subset=None,
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.spatial_unit = spatial_unit
        self.hours = hours
        self.tables = tables
        self.subscriber_identifier = subscriber_identifier
        self.direction = Direction(direction)
        self.statistic = Statistic(statistic.lower())

        column_list = [
            self.subscriber_identifier,
            "location_id",
            "datetime",
            *self.direction.required_columns,
        ]

        self.unioned_query = location_joined_query(
            EventsTablesUnion(
                self.start,
                self.stop,
                tables=self.tables,
                columns=column_list,
                hours=hours,
                subscriber_identifier=subscriber_identifier,
                subscriber_subset=subscriber_subset,
            ),
            spatial_unit=self.spatial_unit,
            time_col="datetime",
        )

        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):
        loc_cols = ", ".join(self.spatial_unit.location_id_columns)

        where_clause = make_where(self.direction.get_filter_clause())

        return f"""
        SELECT subscriber, {self.statistic:events} AS value
        FROM (
            SELECT subscriber, {loc_cols}, COUNT(*) AS events
            FROM ({self.unioned_query.get_query()}) U
            {where_clause}
            GROUP BY subscriber, {loc_cols}
        ) U
        GROUP BY subscriber
        """
