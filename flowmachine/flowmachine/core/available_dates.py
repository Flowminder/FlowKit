# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import textwrap
from typing import List
from .query import Query

__all__ = ["AvailableDates"]

# TODO: this should live somewhere else where it can be re-used by all queries
# and also where validation can happen centrally (rather than having to be
# repeated and scattered in various classes using it).
SUPPORTED_EVENT_TYPES = ["calls", "sms", "mds", "topups"]


class AvailableDates(Query):
    """
    Returns the available dates for each of the given event types.
    """

    def __init__(self, event_types=None):

        if event_types is None:
            event_types = SUPPORTED_EVENT_TYPES

        if not set(event_types).issubset(SUPPORTED_EVENT_TYPES):
            unsupported_event_types = set(event_types).difference(SUPPORTED_EVENT_TYPES)
            raise ValueError(f"Unsupported event types: {unsupported_event_types}")

        self.event_types = event_types

    @property
    def column_names(self) -> List[str]:
        return ["event_type", "dates"]

    def _make_query(self):
        schema = "events"
        sql = textwrap.dedent(
            f"""
            SELECT
                event_type,
                ARRAY(
                    SELECT
                        TO_CHAR(SUBSTRING(table_name from '.{{8}}$')::date, 'YYYY-MM-DD') as date
                    FROM information_schema.tables
                    WHERE table_name SIMILAR TO (event_type || '_[0-9]{{8}}')
                    AND table_schema='{schema}'
                    ORDER BY date
                ) as dates
            FROM UNNEST(ARRAY {self.event_types}) as event_type
            """
        )
        return sql
