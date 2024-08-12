# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import warnings
from typing import List, Union

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.utils import parse_datestring
from flowmachine.core.statistic_types import Statistic


class JoinedSpatialAggregate(GeoDataMixin, Query):
    """
    Creates spatially aggregated data from two objects, one of which is
    a metric of subscribers, and the other of which represents the subscribers'
    locations.

    A general class that join metric information about a subscriber with location
     information about a subscriber and aggregates to the geometric level.

    Parameters
    ----------
    metric : Query
        A query object that represents a subscriber level metric such
        as radius of gyration. The underlying data must have a 'subscriber'
        column. All other columns must be numeric and will be aggregated.
    locations : Query
        A query object that represents the locations of subscribers.
        Must have a 'subscriber' column, and a 'spatial_unit' attribute.
    method : Statistic, or "distr"
            Method of aggregation.

    Examples
    --------
        >>>  mfl = subscribers.MostFrequentLocation('2016-01-01',
                                              '2016-01-04',
                                              spatial_unit=AdminSpatialUnit(level=3))
        >>> rog = subscribers.RadiusOfGyration('2016-01-01',
                                         '2016-01-04')
        >>> sm = JoinedSpatialAggregate(metric=rog, locations=mfl)
        >>> sm.head()
                name     rog
            0   Rasuwa   157.200039
            1   Sindhuli 192.194037
            2   Humla    123.676914
            3   Gulmi    163.980299
            4   Jumla    144.432886
            ...
    """

    allowed_methods = [*[f"{stat}" for stat in Statistic], "distr"]

    def __init__(
        self, *, metric, locations, method: Union[Statistic, str] = Statistic.AVG
    ):
        self.metric = metric
        self.locations = locations
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = locations.spatial_unit
        self.method = method.lower()
        if self.method != "distr":
            self.method = Statistic(self.method)
        if self.method not in self.allowed_methods:
            raise ValueError(
                f"{method} is not recognised method, must be one of {self.allowed_methods}"
            )
        try:
            if (
                parse_datestring(self.metric.start).date()
                != parse_datestring(self.locations.start).date()
            ):
                warnings.warn(
                    f"{self.metric} and {self.locations} have different start dates: {self.metric.start}, and {self.locations.start}"
                )
            if (
                parse_datestring(self.metric.stop).date()
                != parse_datestring(self.locations.stop).date()
            ):
                warnings.warn(
                    f"{self.metric} and {self.locations} have different stop dates: {self.metric.stop}, and {self.locations.stop}"
                )
        except AttributeError:
            pass  # Not everything has a start/stop date
        super().__init__()

    def _make_query(self):
        metric_cols = self.metric.column_names
        metric_cols_no_subscriber = [cn for cn in metric_cols if cn != "subscriber"]
        location_cols = self.spatial_unit.location_id_columns

        # Make some comma separated strings for use in the SQL query
        metric_list = ", ".join(f"metric.{c}" for c in metric_cols)

        # We need to do this because it may be the case that
        # a location is identified by more than one column, as
        # is the case for lon-lat values
        loc_list = ", ".join(f"location.{lc}" for lc in location_cols)
        loc_list_no_schema = ", ".join(location_cols)

        metric = self.metric.get_query()
        location = self.locations.get_query()

        joined = f"""
        SELECT
            {metric_list},
            {loc_list}
        FROM
            ({metric}) AS metric
        INNER JOIN
            ({location}) AS location
        ON metric.subscriber=location.subscriber
        """

        if self.method == "distr":
            grouped = " UNION ".join(
                f"""
                SELECT
                    {loc_list_no_schema},
                    '{mc}' AS metric,
                    key,
                    value / SUM(value) OVER (PARTITION BY {loc_list_no_schema}) AS value
                FROM (
                    SELECT
                        {loc_list_no_schema},
                        {mc}::text AS key,
                        COUNT(*) AS value
                    FROM joined
                    GROUP BY {loc_list_no_schema}, {mc}
                ) _
                """
                for mc in metric_cols_no_subscriber
            )

            grouped = f"WITH joined AS ({joined}) {grouped}"

        else:
            av_cols = ", ".join(
                f"{self.method:{mc}} AS {mc}" for mc in metric_cols_no_subscriber
            )

            # Now do the group by bit
            grouped = f"""
            SELECT
                {loc_list_no_schema},
                {av_cols}
            FROM ({joined}) AS joined
            GROUP BY {loc_list_no_schema}
            """

        return grouped

    @property
    def column_names(self) -> List[str]:
        if self.method == "distr":
            return self.spatial_unit.location_id_columns + ["metric", "key", "value"]
        else:
            return self.spatial_unit.location_id_columns + [
                cn for cn in self.metric.column_names if cn != "subscriber"
            ]
