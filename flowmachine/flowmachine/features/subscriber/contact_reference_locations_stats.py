# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Statistics for the distance between subscriber's own modal
location and its contacts' modal location.
"""

from .metaclasses import SubscriberFeature

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}


class ContactReferenceLocationStats(SubscriberFeature):
    """
    This class calculates statistics of the distance between a subscriber's reference point and its contacts' reference point.

    Parameters
    ----------
    contact_balance: flowmachine.features.ContactBalance
        An instance of `ContactBalance` which lists the contacts of the
        targeted subscribers along with the number of events between them.
    contact_locations: flowmachine.core.Query
        A flowmachine Query instance that contains a subscriber column. In
        addition to that the query must have a spatial unit or the target
        geometry column that contains the subscribers' reference locations.
    statistic : {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to sum, aggregation statistic over the durations.
    geom_column:
        The column containing the subscribers' reference locations. This is
        required if the Query does not contain a spatial unit with 'lon' and
        'lat' columns.

    Example
    -------

    >>> s = ContactModalLocationDistance("2016-01-01", "2016-01-03", statistic="avg")
    >>> s.get_dataframe()

        subscriber           value
    gwAynWXp4eWvxGP7    298.721500
    GnyZMedmKQ4X78Wa    290.397556
    BKMy1nYEZpnoEA7G     78.919136
    m4L326vrwE6elJxQ    249.033988
    NG1km5NzBg5JD8nj    188.679378
               ...           ...
    """

    def __init__(
        self, contact_balance, contact_locations, statistic="avg", geom_column=None
    ):

        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        self.contact_locations_query = contact_locations
        self.contact_balance_query = contact_balance
        self.geom_column = geom_column

        if "subscriber" not in self.contact_locations_query.column_names:
            raise ValueError(
                "The contact locations query must have a subscriber column."
            )

        if self.geom_column is None:
            try:
                self.contact_locations_query.spatial_unit.verify_criterion(
                    "has_lon_lat_columns"
                )
            except AttributeError:
                raise ValueError(
                    "The contact locations must have a spatial unit whenever the geometry column is not specified."
                )

        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):

        if self.geom_column:
            loc_cols = lambda table: f"{table}.{self.geom_column}"
        else:
            loc_cols = lambda table: f"ST_POINT({table}.lon, {table}.lat)"

        sql = f"""
        WITH L AS (
                SELECT C.subscriber, C.msisdn_counterpart, C.subscriber_geom_point, {loc_cols('L')} AS msisdn_counterpart_geom_point
                FROM (
                    SELECT C.subscriber, C.msisdn_counterpart, {loc_cols('L')} AS subscriber_geom_point
                    FROM ({self.contact_balance_query.get_query()}) C
                    JOIN ({self.contact_locations_query.get_query()}) L
                    ON C.subscriber = L.subscriber
                ) C
                JOIN ({self.contact_locations_query.get_query()}) L
                ON C.msisdn_counterpart = L.subscriber
        ),
        D AS (
            SELECT
                subscriber_geom_point, msisdn_counterpart_geom_point,
                ST_Distance(subscriber_geom_point::geography, msisdn_counterpart_geom_point::geography) / 1000 AS distance
            FROM (SELECT DISTINCT subscriber_geom_point, msisdn_counterpart_geom_point FROM L) L
        )
        SELECT subscriber, {self.statistic}(distance) AS value
        FROM (
            SELECT C.subscriber, C.msisdn_counterpart, D.distance
            FROM ({self.contact_balance_query.get_query()}) C, L, D
            WHERE
                C.subscriber = L.subscriber AND
                C.msisdn_counterpart = L.msisdn_counterpart AND
                L.subscriber_geom_point = D.subscriber_geom_point AND
                L.msisdn_counterpart_geom_point = D.msisdn_counterpart_geom_point
        ) D
        GROUP BY subscriber
        """

        return sql
