# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import structlog
import datetime
from typing import List

from ...core import Query, Table
from ...core.sqlalchemy_utils import (
    get_sqlalchemy_table_definition,
    make_sqlalchemy_column_from_flowmachine_column_description,
    get_sql_string,
)

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class SubscriberSigntings(Query):
    """
    TODO - add something here about how to use the object
    """

    def __init__(self, start, stop, *, subscriber_identifier="msisdn"):
        # Set start stop dates
        if isinstance(start, datetime.date):
            start = start.strftime("%Y-%m-%d")
        if isinstance(stop, datetime.date):
            stop = stop.strftime("%Y-%m-%d")

        self.start = start
        self.stop = stop

        # Chose the identifer from interactions.subscribers table
        # rather than subscriber_sightings_fact - as this will allow
        # us to select the required field.
        table = Table("interactions.subscribers", columns=[subscriber_identifier])
        self.columns = set(table.column_names)

        self.sqlalchemy_table = get_sqlalchemy_table_definition(
            table.fully_qualified_table_name, engine=Query.connection.engine
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return []

    def _make_query_with_sqlalchemy(self):

        sqlalchemy_columns = [
            make_sqlalchemy_column_from_flowmachine_column_description(
                self.sqlalchemy_table, column_str
            )
            for column_str in self.columns
        ]

        return "TODO - add the SQL"

    _make_query = _make_query_with_sqlalchemy
