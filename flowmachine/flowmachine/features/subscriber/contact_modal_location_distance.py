# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Statistics for the distance between subscriber's own modal
location and its contacts' modal location.
"""

from .metaclasses import SubscriberFeature

from .modal_location import ModalLocation
from .contact_balance import ContactBalance
from ..spatial.distance_matrix import DistanceMatrix
from .daily_location import daily_location

from flowmachine.utils.utils import get_columns_for_level, list_of_dates

valid_stats = {"count", "sum", "avg", "max", "min", "median", "stddev", "variance"}


class ContactModalLocationDistance(SubscriberFeature):
    """
    This class calculates statistics for the distance between subscriber's own modal
    location and its contacts' modal location.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    statistic : {'count', 'sum', 'avg', 'max', 'min', 'median', 'mode', 'stddev', 'variance'}, default 'sum'
        Defaults to sum, aggregation statistic over the durations.
    method : str, default 'last'
        The method by which to calculate the location of the subscriber.
        This can be either 'most-common' or last. 'most-common' is
        simply the modal location of the subscribers, whereas 'lsat' is
        the location of the subscriber at the time of the final call in
        the data.
    contact_balance: features.subscriber.ContactBalance, default None.
        An instance of ContactBalance. If an instance is not provide, the
        ContactBalance is instantiated with the same parameters as for the
        features.subscriber.ModalLocation
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    direction : {'in', 'out', 'both'}, default 'out'
        Whether to consider calls made, received, or both. Defaults to 'out'.
    column_name : str
        Optionally specify a non-default column name. Required if level is 'polygon'.

    Example
    -------

    >>> s = ContactModalLocationDistance("2016-01-01", "2016-01-03", statistic="avg")
    >>> s.get_dataframe()

        subscriber  distance_avg
    gwAynWXp4eWvxGP7    298.721500
    GnyZMedmKQ4X78Wa    290.397556
    BKMy1nYEZpnoEA7G     78.919136
    m4L326vrwE6elJxQ    249.033988
    NG1km5NzBg5JD8nj    188.679378
               ...           ...
    """

    def __init__(
        self,
        start,
        stop,
        statistic="avg",
        method="last",
        *,
        contact_balance=None,
        tables = "all",
        direction="both",
        hours="all",
        subscriber_subset=None,
        size=None,
    ):

        self.start = start
        self.stop = stop
        self.direction = direction
        self.hours = hours
        self.tables = tables
        self.method = method

        self.statistic = statistic.lower()
        if self.statistic not in valid_stats:
            raise ValueError(
                "{} is not a valid statistic. Use one of {}".format(
                    self.statistic, valid_stats
                )
            )

        self.distance_matrix_query = DistanceMatrix(
            level="versioned-cell",
        )

        if contact_balance:
            if contact_balance.subscriber_identifier not in {"msisdn"}:
                raise ValueError(f"""
                The only `subscriber_identifier` allowed is the msisdn, because
                it is the only identifier capable of identifying counterparts
                for all transation types. Please use a ContactBalance
                instantiated with msisdn for the `subscriber_identifier`.
                """)
            self.contact_balance_query = contact_balance
        else:
            self.contact_balance_query = ContactBalance(
                self.start,
                self.stop,
                hours=self.hours,
                tables=self.tables,
                subscriber_identifier="msisdn",
                direction=self.direction,
                exclude_self_calls=True,
                subscriber_subset=subscriber_subset,
            )

        print(self.contact_balance_query.get_query())

        self.modal_location_query = ModalLocation(*[
            daily_location(
                d,
                level="versioned-cell",
                hours=self.hours,
                method=self.method,
                table=self.tables,
                subscriber_identifier="msisdn",
                subscriber_subset=self.contact_balance_query.counterparts_subset(include_subscribers=True),
            )
            for d in list_of_dates(self.start, self.stop)
        ])

        super().__init__()


    def _make_query(self):

        loc_cols = get_columns_for_level(self.modal_location_query.level)

        subscriber_loc_cols_before_merge = ", ".join(
            [f"M.{i} AS subscriber_{i}" for i in loc_cols]
        )
        subscriber_loc_cols_after_merge = ", ".join(
            [f"C.subscriber_{i}" for i in loc_cols]
        )
        counterpart_loc_cols = ", ".join(
            [f"M.{i} AS msisdn_counterpart_{i}" for i in loc_cols]
        )

        last_merge_clause = " AND ".join(
            [f"C.subscriber_{i} = D.{i}_from" for i in loc_cols]
            + [f"C.msisdn_counterpart_{i} = D.{i}_to" for i in loc_cols]
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
