# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from concurrent.futures import Future

from flowmachine_core.core.context import get_db, get_redis
from flowmachine_core.core.query_state import QueryStateMachine
from threading import Thread
import pandas as pd


def test_returns_future(test_query):
    """
    Getting a dataframe async returns a future.
    """

    assert isinstance(test_query.get_dataframe_async(), Future)


def test_get_dataframe(test_query):
    """
    Get a dataframe from the Future returned by asynchronously
    getting a dataframe.
    """

    assert isinstance(test_query.get_dataframe_async().result(), pd.DataFrame)


def test_double_store(test_query):
    """
    Storing a query twice doesn't raise an error.
    """

    test_query.store().result()
    test_query.store().result()


def test_store_async(test_query):
    """
    Storing a query async stores it (eventually).
    """

    schema = "cache"
    table_name = test_query.fully_qualified_table_name.split(".")[1]
    store_future = test_query.store()
    store_future.result()
    assert get_db().has_table(table_name, schema=schema)
    assert table_name in test_query.get_query()


def test_get_query_blocks_on_store(test_query):
    """
    If a store is running get_query should block.
    """
    test_query.store().result()
    timer = []

    def unlock(timer, redis, db_id):
        qsm = QueryStateMachine(redis, test_query.query_id, db_id)
        qsm.enqueue()
        for i in range(101):
            timer.append(i)
        qsm.execute()
        qsm.finish()

    timeout = Thread(target=unlock, args=(timer, get_redis(), get_db().conn_id))
    timeout.start()
    test_query.get_query()
    assert len(timer) == 101
    timeout.join()


def test_blocks_on_store_cascades(test_query, nested_test_query):
    """
    If a store is running on a query that is used
    in a another query, that query should wait.
    """
    store_future = test_query.store()
    store_future.result()
    timer = []

    def unlock(timer, redis, db_id):
        qsm = QueryStateMachine(redis, test_query.query_id, db_id)
        qsm.enqueue()
        for i in range(101):
            timer.append(i)
        qsm.execute()
        qsm.finish()

    timeout = Thread(target=unlock, args=(timer, get_redis(), get_db().conn_id))
    timeout.start()
    nested_test_query.get_query()
    assert len(timer) == 101
    timeout.join()
