# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import structlog
import datetime
from typing import List
from sqlalchemy import select, join

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

        # Setup the main subscriber_sightings_fact table
        self.mainTable = Table(
            "interactions.subscriber_sightings_fact", columns=["timestamp", "cell_id"]
        )
        self.sqlalchemy_mainTable = get_sqlalchemy_table_definition(
            self.mainTable.fully_qualified_table_name, engine=Query.connection.engine
        )

        # Chose the identifer from interactions.subscribers table
        # rather than subscriber_sightings_fact - as this will allow
        # us to select the required field.
        self.subTable = Table(
            "interactions.subscriber", columns=[subscriber_identifier]
        )
        self.sqlalchemy_subTable = get_sqlalchemy_table_definition(
            self.subTable.fully_qualified_table_name, engine=Query.connection.engine
        )

        # For referencing - we can get all the column name here - perhaps this isn't needed
        self.columns = self.mainTable.column_names + self.subTable.column_names

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.columns

    def _make_query_with_sqlalchemy(self):
        subscriber_id = make_sqlalchemy_column_from_flowmachine_column_description(
            self.sqlalchemy_mainTable, "subscriber_id"
        )
        id = make_sqlalchemy_column_from_flowmachine_column_description(
            self.sqlalchemy_subTable, "id"
        )

        sqlalchemy_columns = [
            make_sqlalchemy_column_from_flowmachine_column_description(
                self.sqlalchemy_mainTable, column_str
            )
            for column_str in self.mainTable.column_names
        ]

        return select(sqlalchemy_columns).select_from(
            self.sqlalchemy_mainTable.join(
                self.sqlalchemy_subTable, subscriber_id == id
            )
        )

    _make_query = _make_query_with_sqlalchemy
