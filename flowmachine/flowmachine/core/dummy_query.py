# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
A dummy query class, primarily useful for testing.
"""

import structlog
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

    @property
    def md5(self):
        # Prefix the usual md5 hash with 'dummy_query' to make it obvious
        # that this is not a regular query.
        md5_hash = super().md5
        return f"dummy_query_{md5_hash}"

    def _make_query(self):
        return "SQL_of_dummy_query"

    @property
    def column_names(self):
        return []

    @property
    def is_stored(self):
        try:
            status = self._dummy_stored_status
        except AttributeError:
            status = False
            self._dummy_stored_status = status
        return status

    def store(self):
        logger.debug(
            "Storing dummy query by marking the query state as 'finished' (but without actually writing to the database)."
        )
        q_state_machine = QueryStateMachine(self.redis, self.md5)
        q_state_machine.enqueue()
        q_state_machine.execute()
        self._dummy_stored_status = True
        q_state_machine.finish()

    def explain(self, format="text", analyse=False):
        """
        Override Query.explain so that no SQL is executed
        """
        if format.upper() != "JSON":
            raise NotImplementedError(
                f"Only format='json' is supported by {self.__class__.__name__}.explain()"
            )
        return [{"Plan": {"Total Cost": 0.0}}]

    def __getstate__(self):
        """
        Override Query.__getstate__ to remove _dummy_stored_status from state.
        """
        state = super().__getstate__()
        try:
            del state["_dummy_stored_status"]
        except:
            pass  # Not a problem if it doesn't exist
        return state
