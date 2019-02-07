# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Statistics for the distance between subscriber's own modal
location and its contacts' modal location.
"""

from .contact_balance import ContactBalance
from flowmachine.utils.utils import get_columns_for_level, list_of_dates
from .metaclasses import SubscriberFeature
from .daily_location import daily_location
from .modal_location import ModalLocation
from ..spatial.distance_matrix import DistanceMatrix

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}

class ContactModalLocationDistance(SubscriberFeature):
    """
    This class calculates statistics for the distance between subscriber's own modal
    location and its contacts' modal location.

    Parameters
    ----------
    statistic :  {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'avg'
        Defaults to sum, aggregation statistic over the durations.
    modal_location: features.subscriber.ModalLocation
        An instance of ModalLocation.
    contact_balance: features.subscriber.ContactBalance
        An instance of ContactBalance.

    Example
    -------

    >>> s = ContactModalLocationDistance(modal_location, contact_balance, statistic="avg")
    >>> s.get_dataframe()

        subscriber  distance_avg
    gwAynWXp4eWvxGP7    298.721500
    GnyZMedmKQ4X78Wa    290.397556
    BKMy1nYEZpnoEA7G     78.919136
    m4L326vrwE6elJxQ    249.033988
    NG1km5NzBg5JD8nj    188.679378
               ...           ...
    """

    def __init__(self, modal_location, contact_balance, statistic="avg"):

        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )
        self.modal_location_query = modal_location
        self.contact_balance_query = contact_balance
        self.distance_matrix_query = DistanceMatrix(
            level=self.modal_location_query.level
        )

    def _make_query(self):

        loc_cols = get_columns_for_level(self.modal_location_query.level)

        subscriber_loc_cols_before_merge = ", ".join([f"M.{i} AS subscriber_{i}" for i in loc_cols])
        subscriber_loc_cols_after_merge = ", ".join([f"C.subscriber_{i}" for i in loc_cols])
        counterpart_loc_cols = ", ".join([f"M.{i} AS msisdn_counterpart_{i}" for i in loc_cols])

        last_merge_clause = " AND ".join(
                [f"C.subscriber_{i} = D.{i}_from" for i in loc_cols] +
                [f"C.msisdn_counterpart_{i} = D.{i}_to" for i in loc_cols]
        )

        sql = f"""
        SELECT subscriber, {self.statistic}(distance) AS distance_{self.statistic}
        FROM (
            SELECT C.subscriber, C.msisdn_counterpart, D.distance
            FROM (
                SELECT C.subscriber, C.msisdn_counterpart, {subscriber_loc_cols_after_merge}, {counterpart_loc_cols}
                FROM (
                    SELECT C.subscriber, C.msisdn_counterpart, {subscriber_loc_cols_before_merge}
                    FROM ({self.contact_balance_query.get_query()}) C
                    JOIN ({self.modal_location_query.get_query()}) M
                    ON C.subscriber = M.subscriber
                ) C JOIN ({self.modal_location_query.get_query()}) M
                ON C.msisdn_counterpart = M.subscriber
            ) C JOIN ({self.distance_matrix_query.get_query()}) D
            ON {last_merge_clause}
        ) D
        GROUP BY subscriber
        """

        return sql





