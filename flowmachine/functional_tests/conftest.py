import pandas as pd
import pytest

import flowmachine
from flowmachine.core import Query
from flowmachine.core.cache import reset_cache


@pytest.fixture(autouse=True)
def flowmachine_connect():
    con = flowmachine.connect()
    yield con
    reset_cache(con)
    con.engine.dispose()  # Close the connection
    Query.redis.flushdb()  # Empty the redis
    del Query.connection  # Ensure we recreate everything at next use


@pytest.fixture
def get_dataframe(flowmachine_connect):
    yield lambda query: pd.read_sql_query(
        query.get_query(), con=flowmachine_connect.engine
    )
