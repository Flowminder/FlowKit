# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Calculates various entropy metrics for subscribers with a specified time
period.
"""

from abc import ABCMeta
from .metaclasses import SubscriberFeature
from ..utilities.subscriber_locations import subscriber_locations

class BaseEntropy(SubscriberFeature, metaclass=ABCMeta):
    """ Base query for calculating entropy of subscriber features. """
    __metaclass__ = abc.ABCMeta

    def _make_query(self):

        return f"""
        SELECT
            subscriber,
            -1 * SUM( relative_freq / LN( relative_freq ) ) AS entropy
        FROM ({self._relative_freq_query}) u
        GROUP BY subscriber
        """

    @property
    def _absolute_freq_query(self):

        raise NotImplementedError

    @property
    def _relative_freq_query(self):
        return f"""
        SELECT
            subscriber,
            absolute_freq / ( SUM( absolute_freq ) OVER ( PARTITION BY subscriber ) ) AS relative_freq
        FROM ({self._absolute_freq_query}) u
        """



class SubscriberPeriodicEntropy(BaseEntropy):

    def __init__(
        self,
        start,
        stop,
        phase="hour",
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

        if direction not in {"in", "out", "both"}:
            raise ValueError("{} is not a valid direction.".format(self.direction))

        if self.direction == "both":
            column_list = [self.subscriber_identifier, "datetime"]
            self.tables = tables
        else:
            column_list = [self.subscriber_identifier, "datetime", "outgoing"]
            self.tables = self._parse_tables_ensuring_direction_present(tables)

        # extracted from the POSTGRES manual
        allowed_phases=("century", "day", "decade", "dow", "doy", "epoch",
                "hour", "isodow", "isoyear", "microseconds", "millennium",
                "milliseconds", "minute", "month", "quarter", "second",
                "timezone", "timezone_hour", "timezone_minute", "week",
                "year",)

        if phase not in allowed_phases:
            raise ValueError(f"{phase} is not a valid phase. Choose one of {allowed_phases}")

        self.phase = phase

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

    def _parse_tables_ensuring_direction_present(self, tables):

        if isinstance(tables, str) and tables.lower() == "all":
            tables = [f"events.{t}" for t in self.connection.subscriber_tables]
        elif type(tables) is str:
            tables = [tables]
        else:
            tables = tables

        parsed_tables = []
        tables_lacking_direction_column = []
        for t in tables:
            if "outgoing" in Table(t).column_names:
                parsed_tables.append(t)
            else:
                tables_lacking_direction_column.append(t)

        if tables_lacking_direction_column:
            raise MissingDirectionColumnError(tables_lacking_direction_column)

        return parsed_tables

    @property
    def _absolute_freq_query(self):

        where_clause = ""
        if self.direction != "both":
            where_clause = (
                f"WHERE outgoing IS {'TRUE' if self.direction == 'out' else 'FALSE'}"
            )

        return f"""
        SELECT subscriber, COUNT(*) AS absolute_freq FROM
        (self.unioned_query.get_query()) u
        GROUP BY subscriber, EXTRACT( {self.phase} FROM datetime )
        """

class SubscriberLocationEntropy(BaseEntropy):

    def __init__(
        self,
        start,
        stop,
        *,
        level="cell",
        column_name=None,
        subscriber_identifier="msisdn",
        hours="all",
        subscriber_subset=None,
        tables="all",
        ignore_nulls=True,
    ):

        self.subscriber_locations = subscriber_locations(
            start=start,
            stop=stop,
            level=level,
            column_name=column_name,
            table=tables,
            hours=hours,
            subscriber_identifier=subscriber_identifier,
            subscriber_subset=subscriber_subset,
            ignore_nulls=ignore_nulls
        )
        super().__init__()

    @property
    def _absolute_freq_query(self):

        location_cols = [cn for cn in
                self.subscriber_locations.locations.column_names if cn !=
                "subscriber"]

        return f"""
        SELECT subscriber, COUNT(*) AS absolute_freq FROM
        (self.subscriber_locations.get_query()) u
        GROUP BY subscriber, {location_cols}
        """


class SubscriberContactEntropy(BaseEntropy):

    def __init__(
        self,
        start,
        stop,
        *,
        subscriber_identifier="msisdn",
        direction="both",
        hours="all",
        subscriber_subset=None,
        tables="all",
    ):

        self.contact_balance = ContactBalance(
            start=start,
            stop=stop,
            hours=hours,
            table=tables,
            subscriber_identifier=subscriber_identifier,
            direction=direction,
            exclude_self_calls=exclude_self_calls,
            subscriber_subset=subscriber_subset,
        )

    @property
    def _relative_freq_query(self):

        return f"""
        SELECT subscriber, proportion AS relative_freq FROM
        ({self.contact_balance.get_query()}) u
        """







