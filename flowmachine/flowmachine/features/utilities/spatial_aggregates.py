# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility classes for performing spatial aggregate
operations in CDRs.
"""
import warnings
from typing import List

from ...core.query import Query
from ...core.mixins import GeoDataMixin
from flowmachine.utils import parse_datestring


class SpatialAggregate(GeoDataMixin, Query):
    """
    Class representing the result of spatially aggregating
    a locations object. A locations object represents the
    location of multiple subscribers. This class represents the output
    of aggregating that data spatially.

    Parameters
    ----------
    locations : subscriber location query
    """

    def __init__(self, *, locations):

        self.locations = locations
        self.level = locations.level
        self.column_name = locations.column_name
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.locations.column_names[1:] + ["total"]

    def _make_query(self):

        aggregate_cols = self.locations.column_names[1:]

        sql = """
        SELECT
            {agg_cols},
            count(*) AS total
        FROM
            ({to_agg}) AS to_agg
        GROUP BY
            {agg_cols}
        """.format(
            to_agg=self.locations.get_query(), agg_cols=",".join(aggregate_cols)
        )

        return sql


class JoinedSpatialAggregate(GeoDataMixin, Query):
    """
    Creates spatially aggregated data from two objects, one of which is
    a metric of subscribers, and the other of which represents the subscribers
    location.

    A general class that join metric information about a subscriber with location
     information about a subscriber and aggregates to the geometric level.

    Parameters
    ----------
    metric : Query
        A query object that represents a subscriber level metric such
        as radius of gyration. The underlying data must have the
        first column as 'subscriber'. All subsequent columns must be
        numeric and will be meaned.
    locations : Query
        A query object that represents the locations of subscribers.
        The first column should be 'subscriber', and subsequent columns
        locations.
    method : {"mean", "median", "mode"}
            Method of aggregation.

    Examples
    --------
        >>>  mfl = subscribers.MostFrequentLocation('2016-01-01',
                                              '2016-01-04',
                                              level='admin3')
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

    allowed_methods = {"avg", "max", "min", "median", "mode", "stddev", "variance"}

    def __init__(self, *, metric, locations, method="mean"):
        self.metric = metric
        self.locations = locations
        self.level = locations.level
        self.column_name = locations.column_name
        self.method = method.lower()
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
        location_cols = [cn for cn in self.locations.column_names if cn != "subscriber"]

        # Make some comma separated strings for use in the SQL query
        metric_list = ", ".join(f"metric.{c}" for c in metric_cols)

        # We need to do this because it may be the case that
        # a location is identified by more than one column, as
        # is the case for lat-lon values
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

        if self.method == "mode":
            av_cols = ", ".join(
                f"pg_catalog.mode() WITHIN GROUP(ORDER BY {mc}) AS {mc}"
                for mc in metric_cols[1:]
            )
        else:
            av_cols = ", ".join(
                f"{self.method}({mc}) AS {mc}" for mc in metric_cols[1:]
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
        return [
            cn for cn in self.locations.column_names if cn != "subscriber"
        ] + self.metric.column_names[1:]
