# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

import warnings

from ...core import Table
from ...core.errors.flowmachine_errors import MissingDirectionColumnError
from ..utilities.sets import EventsTablesUnion
from .metaclasses import SubscriberFeature


class SubscriberEventCount(SubscriberFeature):
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
    direction : {'in', 'out', 'both'}, default 'out'
        Whether to consider calls made, received, or both. Defaults to 'out'.
    tables : str or list of strings, default 'all'
        Can be a string of a single table (with the schema)
        or a list of these. The keyword all is to select all
        subscriber tables

    Examples
    --------

    >>> s = SubscriberEventCount("2016-01-01", "2016-01-07", direction="in")
    >>> s.get_dataframe()

             subscriber  event_count
    0  2ZdMowMXoyMByY07           65
    1  MobnrVMDK24wPRzB           81
    2  0Ze1l70j0LNgyY4w           57
    3  Nnlqka1oevEMvVrm           63
    4  4dqenN2oQZExwEK2           59
                    ...          ...
    """

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
        self.start = start
        self.stop = stop
        self.subscriber_identifier = subscriber_identifier
        self.direction = direction
        self.hours = hours

        if direction not in {"in", "out", "both"}:
            raise ValueError("{} is not a valid direction.".format(self.direction))

        if self.direction == "both":
            column_list = [self.subscriber_identifier]
            self.tables = tables
        else:
            column_list = [self.subscriber_identifier, "outgoing"]
            self.tables = self._parse_tables_with_direction(tables)

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

    def _parse_tables_with_direction(self, tables):

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

    def _make_query(self):
        where_clause = ""
        if self.direction != "both":
            where_clause = (
                f"WHERE outgoing IS {'TRUE' if self.direction == 'out' else 'FALSE'}"
            )
        return f"""
        SELECT subscriber, COUNT(*) as event_count FROM
        ({self.unioned_query.get_query()}) u
        {where_clause}
        GROUP BY subscriber
        """
