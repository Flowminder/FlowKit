# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

import warnings

from ...core import Table
from .event_count import EventCount
from .metaclasses import SubscriberFeature

valid_event_types = {"calls", "sms", "mds", "topups", "forwards"}


class ProportionEventType(SubscriberFeature):
    """
    This class returns the proportion of events of a certain type out of all
    events observed during the target period per subscriber.

    Parameters
    ----------
    start, stop : str
         iso-format start and stop datetimes
    event_type: {'calls', 'sms', 'mds', 'topups', 'forwards'}
        The event type for which we are seeking as the proportion of the total.
    hours : 2-tuple of floats, default 'all'
        Restrict the analysis to only a certain set
        of hours within each day.
    subscriber_identifier : {'msisdn', 'imei'}, default 'msisdn'
        Either msisdn, or imei, the column that identifies the subscriber.
    subscriber_subset : str, list, flowmachine.core.Query, flowmachine.core.Table, default None
        If provided, string or list of string which are msisdn or imeis to limit
        results to; or, a query or table which has a column with a name matching
        subscriber_identifier (typically, msisdn), to limit results to.
    direction : {'in', 'out', 'both'}, default 'out'
        Whether to consider calls made, received, or both. Defaults to 'out'.
    tables : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables

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
        event_type,
        *,
        subscriber_identifier="msisdn",
        direction="both",
        hours="all",
        subscriber_subset=None,
        tables="all",
    ):
        self.start = start
        self.stop = stop
        self.subscriber_identifier = subscriber_identifier
        self.direction = direction
        self.hours = hours
        self.tables = tables
        self.event_type = event_type

        if self.event_type not in valid_event_types:
            raise ValueError(f"{self.event_type} is not a valid event type.")

        self.numerator_query = EventCount(
            self.start,
            self.stop,
            subscriber_identifier=self.subscriber_identifier,
            direction=self.direction,
            hours=self.hours,
            subscriber_subset=subscriber_subset,
            tables=f"events.{event_type}",
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
        SELECT D.subscriber, N.event_count::float / D.event_count::float AS value
        FROM ({self.numerator_query.get_query()}) N
        JOIN ({self.denominator_query.get_query()}) D
        ON D.subscriber = N.subscriber
        """
