# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates the proportion of calls that a
subscriber makes during night time. Nocturnal
hour definitions can be specified.



"""
from typing import Union, Tuple

from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.direction_enum import Direction
from flowmachine.utils import make_where, standardise_date


class NocturnalEvents(SubscriberFeature):
    """
    Represents the percentage of events that a subscriber make/receives which
    began at night. The definition of night is configurable.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    hours : tuple of ints, default (20, 4)
        Hours that count as being nocturnal. e.g. (20,4)
        will be the times after 8pm and before 4 am.
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

    Examples
    --------

    >>> s = NocturnalEvents("2016-01-01", "2016-01-02")
    >>> s.get_dataframe()

          subscriber                 value
    2ZdMowMXoyMByY07              0.000000
    MobnrVMDK24wPRzB             40.000000
    0Ze1l70j0LNgyY4w             16.666667
    Nnlqka1oevEMvVrm             33.333333
    4dqenN2oQZExwEK2             83.333333
                 ...                   ...
    """

    def __init__(
        self,
        start,
        stop,
        hours: Tuple[int, int] = (20, 4),
        *,
        subscriber_identifier="msisdn",
        direction: Union[str, Direction] = Direction.BOTH,
        subscriber_subset=None,
        tables="all",
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.subscriber_identifier = subscriber_identifier
        self.direction = Direction(direction)
        self.hours = hours
        self.tables = tables

        column_list = [
            self.subscriber_identifier,
            "datetime",
            *self.direction.required_columns,
        ]

        self.unioned_query = EventsTablesUnion(
            self.start,
            self.stop,
            tables=self.tables,
            columns=column_list,
            hours="all",
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
        )
        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):
        where_clause = make_where(self.direction.get_filter_clause())

        sql = f"""
        SELECT
            subscriber,
            AVG(nocturnal)*100 AS value
        FROM (
            SELECT
                subscriber,
                CASE
                    WHEN extract(hour FROM datetime) >= {self.hours[0]}
                      OR extract(hour FROM datetime) < {self.hours[1]}
                    THEN 1
                ELSE 0
            END AS nocturnal
            FROM ({self.unioned_query.get_query()}) U
            {where_clause}
        ) U
        GROUP BY subscriber
        """

        return sql
