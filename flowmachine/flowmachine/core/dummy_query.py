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
    def query_id(self):
        # Prefix the usual query_id hash with 'dummy_query' to make it obvious
        # that this is not a regular query.
        md5_hash = super().query_id
        return f"dummy_query_{md5_hash}"

    def _make_query(self):
        return "SQL_of_dummy_query"

    @property
    def column_names(self):
        return []

    def store(self):
        logger.debug(
            "Storing dummy query by marking the query state as 'finished' (but without actually writing to the database)."
        )
        q_state_machine = QueryStateMachine(self.redis, self.query_id)
        q_state_machine.enqueue()
        q_state_machine.execute()
        q_state_machine.finish()
