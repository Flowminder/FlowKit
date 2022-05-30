# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List, Union, Optional, Tuple

from flowmachine.features.utilities.events_tables_union import EventsTablesUnion
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.direction_enum import Direction
from flowmachine.utils import make_where, standardise_date


class EventCount(SubscriberFeature):
    """
    This class returns the event count per subscriber within the period,
    optionally limited to only incoming or outgoing events.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
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

    Examples
    --------

    >>> s = EventCount("2016-01-01", "2016-01-07", direction="in")
    >>> s.get_dataframe()

          subscriber        value
    2ZdMowMXoyMByY07           65
    MobnrVMDK24wPRzB           81
    0Ze1l70j0LNgyY4w           57
    Nnlqka1oevEMvVrm           63
    4dqenN2oQZExwEK2           59
                 ...          ...
    """

    def __init__(
        self,
        start,
        stop,
        *,
        subscriber_identifier="msisdn",
        direction: Union[str, Direction] = Direction.BOTH,
        hours: Optional[Tuple[int, int]] = None,
        subscriber_subset=None,
        tables="all",
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.subscriber_identifier = subscriber_identifier
        self.direction = Direction(direction)
        self.hours = hours
        self.tables = tables

        column_list = [self.subscriber_identifier, *self.direction.required_columns]

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
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        return f"""
        SELECT subscriber, COUNT(*) as value FROM
        ({self.unioned_query.get_query()}) u
        {make_where(self.direction.get_filter_clause())}
        GROUP BY subscriber
        """
