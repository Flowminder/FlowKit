# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
A dummy query class, primarily useful for testing.
"""

import structlog

from .context import get_redis, get_db
from .query import Query
from .query_state import QueryStateMachine

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class DummyQuery(Query):
    """
    Dummy query which can be used for testing.
    It does not write to the database.
    """

    def __init__(self, dummy_param):
        self.dummy_param = dummy_param
        super().__init__()

    @property
    def query_id(self):
        return f"dummy_query_{self.dummy_param}"

    def _make_query(self):
        return "SQL_of_dummy_query"

    @property
    def column_names(self):
        return []

    @property
    def is_stored(self):
        """
        Determine dummy 'stored' status from redis, instead of checking the database.
        """
        q_state_machine = QueryStateMachine(
            get_redis(), self.query_id, get_db().conn_id
        )
        return q_state_machine.is_completed

    def store(self, store_dependencies=False):
        logger.debug(
            "Storing dummy query by marking the query state as 'finished' (but without actually writing to the database)."
        )
        q_state_machine = QueryStateMachine(
            get_redis(), self.query_id, get_db().conn_id
        )
        q_state_machine.enqueue()
        q_state_machine.execute()
        q_state_machine.finish()

    def explain(self, format="json", analyse=False):
        """
        Override Query.explain so that no SQL is executed
        """
        if format.upper() != "JSON":
            raise NotImplementedError(
                f"Only format='json' is supported by {self.__class__.__name__}.explain()"
            )
        return [{"Plan": {"Total Cost": 0.0}}]
