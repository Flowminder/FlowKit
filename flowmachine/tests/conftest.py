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
import re
import logging

import flowmachine
from flowmachine.core import Query
from flowmachine.core.cache import reset_cache
from flowmachine.features import EventTableSubset

logger = logging.getLogger()


@pytest.fixture(
    params=[
        {"level": "admin2"},
        {"level": "admin2", "column_name": "admin2name"},
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


def get_string_with_test_parameter_values(item):
    """
    If `item` corresponds to a parametrized pytest test, return a string
    containing the parameter values. Otherwise return an empty string.
    """
    if "parametrize" in item.keywords:
        m = re.search(
            "(\[.*\])$", item.name
        )  # retrieve text in square brackets at the end of the item's name
        if m:
            param_values_str = f" {m.group(1)}"
        else:
            raise RuntimeError(
                f"Test is parametrized but could not extract parameter values from name: '{item.name}'"
            )
    else:
        param_values_str = ""

    return param_values_str


def pytest_itemcollected(item):
    """
    Custom hook which improves stdout logging from from pytest's default.

    Instead of just printing the filename and no description of the test
    (as would be the default) it prints the docstring as the description
    and also adds info about any parameters (if the test is parametrized).
    """
    if item.obj.__doc__:
        item._nodeid = "* " + " ".join(item.obj.__doc__.split())
        item._nodeid += get_string_with_test_parameter_values(item)


@pytest.fixture(autouse=True)
def skip_datecheck(request, monkeypatch):
    """
    Temporarily patches EventTableSubset so that it thinks any date is
    available, _without_ needing to touch the database. This shaves a little
    time off every `daily_location` creation.

    Use the `check_available_dates` py mark on your test to opt-in to date checking.
    """
    run_date_checks = request.node.get_closest_marker("check_available_dates", False)
    if not run_date_checks:
        monkeypatch.setattr(EventTableSubset, "_check_dates", lambda x: True)


@pytest.fixture(autouse=True)
def flowmachine_connect():
    con = flowmachine.connect()
    yield con
    reset_cache(con, Query.redis)
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
    monkeypatch.delenv("FLOWMACHINE_LOG_LEVEL", raising=False)
    monkeypatch.delenv("FLOWDB_PORT", raising=False)
    monkeypatch.delenv("FLOWMACHINE_FLOWDB_USER", raising=False)
    monkeypatch.delenv("FLOWMACHINE_FLOWDB_PASSWORD", raising=False)
    monkeypatch.delenv("FLOWDB_HOST", raising=False)
    monkeypatch.delenv("DB_CONNECTION_POOL_SIZE", raising=False)
    monkeypatch.delenv("DB_CONNECTION_POOL_OVERFLOW", raising=False)
    monkeypatch.delenv("REDIS_HOST", raising=False)
    monkeypatch.delenv("REDIS_PORT", raising=False)
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)


@pytest.fixture
def get_dataframe(flowmachine_connect):
    yield lambda query: pd.read_sql_query(
        f"SELECT {', '.join(query.column_names)} FROM ({query.get_query()}) _",
        con=flowmachine_connect.engine,
    )


@pytest.fixture
def get_column_names_from_run(flowmachine_connect):
    yield lambda query: pd.read_sql_query(
        f"{query.get_query()} LIMIT 0;", con=flowmachine_connect.engine
    ).columns.tolist()


@pytest.fixture
def get_length(flowmachine_connect):
    yield lambda query: len(
        pd.read_sql_query(query.get_query(), con=flowmachine_connect.engine)
    )


class DummyRedis:
    """
    Drop-in replacement for redis.
    """

    def __init__(self):
        self._store = {}
        self.flush = True

    def setnx(self, name, val):
        if name not in self._store:
            self._store[name] = val.encode()

    def eval(self, script, numkeys, name, event):
        current_value = self._store[name]
        try:
            self._store[name] = self._store[event][current_value]
            return self._store[name], current_value
        except KeyError:
            return current_value, None

    def hset(self, key, current, next):
        try:
            self._store[key][current.encode()] = next.encode()
        except KeyError:
            self._store[key] = {current.encode(): next.encode()}

    def set(self, key, value):
        self._store[key] = value.encode()

    def get(self, key):
        return self._store.get(key, None)

    def keys(self):
        return sorted(self._store.keys())

    def flushdb(self):
        if self.flush:  # Set flush attribute to False to simulate concurrent writes
            self._store = {}


@pytest.fixture(scope="function")
def dummy_redis():
    return DummyRedis()
