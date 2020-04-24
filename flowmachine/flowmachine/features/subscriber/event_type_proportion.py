# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import Union

from flowmachine.features.subscriber.event_count import EventCount
from flowmachine.features.subscriber.metaclasses import SubscriberFeature
from flowmachine.features.utilities.direction_enum import Direction
from flowmachine.utils import standardise_date


class ProportionEventType(SubscriberFeature):
    """
    This class returns the proportion of events of a certain type out of all
    events observed during the target period per subscriber in all tables
    specified in `tables`.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    numerator: str or list of strings
        The event tables for which we are seeking as the proportion of the
        total events observed in all tables specified in `tables`.
    numerator_direction : {'in', 'out', 'both'} or Direction, default Direction.BOTH
        Whether to consider events made, received, or both in the numerator. Defaults to 'both'.
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
        Whether to consider events made, received, or both. Defaults to 'both'.
    tables : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables.

    Examples
    --------

    >>> s = ProportionEventType("2016-01-01", "2016-01-07", type="sms")
    >>> s.get_dataframe()

          subscriber       value
    AgB6KR3Levd9Z1vJ    0.351852
    PbN9vQlbEORLmaxd    0.333333
    W6ljak9BpW0Z7ybg    0.271429
    kq7DLQar1nw2glYN    0.260870
    wvMm1EAY0QeEAbN9    0.272727
                 ...         ...
    """

    def __init__(
        self,
        start,
        stop,
        numerator,
        *,
        numerator_direction: Union[str, Direction] = Direction.BOTH,
        subscriber_identifier="msisdn",
        direction: Union[str, Direction] = Direction.BOTH,
        hours="all",
        subscriber_subset=None,
        tables="all",
    ):
        self.start = standardise_date(start)
        self.stop = standardise_date(stop)
        self.subscriber_identifier = subscriber_identifier
        self.direction = Direction(direction)
        self.numerator_direction = Direction(numerator_direction)
        self.hours = hours
        self.tables = tables
        self.numerator = numerator if isinstance(numerator, list) else [numerator]

        self.numerator_query = EventCount(
            self.start,
            self.stop,
            subscriber_identifier=self.subscriber_identifier,
            direction=self.numerator_direction,
            hours=self.hours,
            subscriber_subset=subscriber_subset,
            tables=self.numerator,
        )

        self.denominator_query = EventCount(
            self.start,
            self.stop,
            subscriber_identifier=self.subscriber_identifier,
            direction=self.direction,
            hours=self.hours,
            subscriber_subset=subscriber_subset,
            tables=self.tables,
        )

        super().__init__()

    @property
    def column_names(self):
        return ["subscriber", "value"]

    def _make_query(self):
        return f"""
        SELECT D.subscriber, N.value::float / D.value::float AS value
        FROM ({self.numerator_query.get_query()}) N
        JOIN ({self.denominator_query.get_query()}) D
        ON D.subscriber = N.subscriber
        """
