# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from concurrent.futures import Future
from flowmachine.features.subscriber import *
from threading import Thread
import pandas as pd

from flowmachine.utils.utils import rlock


def test_returns_future():
    """
    Getting a dataframe async returns a future.
    """

    dl = daily_location("2016-01-01")
    assert isinstance(dl.get_dataframe_async(), Future)


def test_get_dataframe():
    """
    Get a dataframe from the Future returned by asynchronously
    getting a dataframe.
    """

    dl = daily_location("2016-01-01")
    assert isinstance(dl.get_dataframe_async().result(), pd.DataFrame)


def test_double_store():
    """
    Storing a query twice doesn't raise an error.
    """

    dl = daily_location("2016-01-01", level="cell")
    dl.store().result()
    dl.store().result()


def test_store_async():
    """
    Storing a query async stores it (eventually).
    """

    schema = "cache"
    dl = daily_location("2016-01-01", level="cell")
    table_name = dl.fully_qualified_table_name.split(".")[1]
    store_future = dl.store()
    store_future.result()
    assert dl.connection.has_table(table_name, schema=schema)
    dl = daily_location("2016-01-01", level="cell")
    assert table_name in dl.get_query()


def test_get_query_blocks_on_store():
    """
    If a store is running get_query should block.
    """
    dl = daily_location("2016-01-01", level="cell")
    dl.store().result()
    timer = []

    def unlock(timer):
        with rlock(dl.redis, dl.md5):
            for i in range(101):
                timer.append(i)

    timeout = Thread(target=unlock, args=(timer,))
    timeout.start()
    dl.get_query()
    assert len(timer) == 101
    timeout.join()


def test_blocks_on_store_cascades():
    """
    If a store is running on a query that is used
    in a another query, that query should wait.
    """
    dl = daily_location("2016-01-01", level="cell")
    dl2 = daily_location("2016-01-02", level="cell")
    store_future = dl.store()
    store_future.result()
    hl = ModalLocation(dl, dl2)
    timer = []

    def unlock(timer):
        with rlock(dl.redis, dl.md5):
            for i in range(101):
                timer.append(i)

    timeout = Thread(target=unlock, args=(timer,))
    timeout.start()
    hl.get_query()
    assert len(timer) == 101
    timeout.join()
