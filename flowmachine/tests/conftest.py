# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Commonly used testing fixtures for flowmachine.
"""

from unittest.mock import Mock

import pandas as pd
import pytest
import logging
import flowmachine
from flowmachine.core import Query
from flowmachine.features import EventTableSubset

logger = logging.getLogger()


@pytest.fixture(
    params=[
        {"level": "admin2"},
        {"level": "versioned-site"},
        {"level": "versioned-cell"},
        {"level": "cell"},
        {"level": "lat-lon"},
        {"level": "grid", "size": 5},
        {
            "level": "polygon",
            "column_name": "admin3pcod",
            "polygon_table": "geography.admin3",
        },
    ],
    ids=lambda x: x["level"],
)
def exemplar_level_param(request):
    """
    A fixture which yields a succession of plausible default parameter
    combinations for levels.

    Parameters
    ----------
    request

    Yields
    ------
    dict

    """
    yield request.param


def pytest_itemcollected(item):
    # improve stdout logging from pytest's default which just prints the
    # filename and no description of the test.
    if item._obj.__doc__:
        item._nodeid = "* " + " ".join(item.obj.__doc__.split())
        if item._genid:
            item._nodeid = item._nodeid.rstrip(".") + f" [{item._genid}]."


@pytest.fixture
def skip_datecheck(monkeypatch):
    """Temporarily patches EventTableSubset so that it thinks any date is
    available, _without_ needing to touch the database. This shaves a little
    time off every `daily_location` creation.
    """
    monkeypatch.setattr(EventTableSubset, "_check_dates", lambda x: True)


@pytest.fixture(autouse=True)
def flowmachine_connect():
    con = flowmachine.connect()
    yield con
    for q in Query.get_stored():  # Remove any cached queries
        q.invalidate_db_cache()
    con.engine.dispose()  # Close the connection
    Query.redis.flushdb()  # Empty the redis
    del Query.connection  # Ensure we recreate everything at next use


@pytest.fixture
def mocked_connections(monkeypatch):
    """
    Fixture which mocks out the setup methods for logger,
    connection, redis and threadpool and yields the mocks.

    Parameters
    ----------
    monkeypatch

    Yields
    ------
    tuple of mocks
        Mocks for _init_logging, Connection, StrictRedis and _start_threadpool

    """
    logging_mock = Mock()
    connection_mock = Mock(return_value=None)
    redis_mock = Mock()
    tp_mock = Mock()
    monkeypatch.delattr("flowmachine.core.query.Query.connection", raising=False)
    monkeypatch.setattr(flowmachine.core.init, "_init_logging", logging_mock)
    monkeypatch.setattr(flowmachine.core.Connection, "__init__", connection_mock)
    monkeypatch.setattr("redis.StrictRedis", redis_mock)
    monkeypatch.setattr(flowmachine.core.init, "_start_threadpool", tp_mock)
    yield logging_mock, connection_mock, redis_mock, tp_mock
    del Query.connection


@pytest.fixture
def clean_env(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FILE", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PW", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("POOL_SIZE", raising=False)
    monkeypatch.delenv("POOL_OVERFLOW", raising=False)
    monkeypatch.delenv("REDIS_HOST", raising=False)
    monkeypatch.delenv("REDIS_PORT", raising=False)


@pytest.fixture
def get_dataframe(flowmachine_connect):
    yield lambda query: pd.read_sql_query(
        query.get_query(), con=flowmachine_connect.engine
    )


@pytest.fixture
def get_length(flowmachine_connect):
    yield lambda query: len(
        pd.read_sql_query(query.get_query(), con=flowmachine_connect.engine)
    )
