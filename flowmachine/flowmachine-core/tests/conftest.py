# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Commonly used testing fixtures for flowmachine_core.
"""
import json
import os
import concurrent.futures.thread
from functools import partial
from json import JSONDecodeError
from pathlib import Path
from typing import List

import pandas as pd
import pytest
import logging
from unittest.mock import Mock, MagicMock

from _pytest.capture import CaptureResult
from approvaltests import verify
from approvaltests.reporters.generic_diff_reporter_factory import (
    GenericDiffReporterFactory,
)

import flowmachine
from flowmachine_core.utility_queries.custom_query import CustomQuery
from flowmachine_core.core.cache import reset_cache
from flowmachine_core.core.context import redis_connection, get_db, get_redis
from flowmachine_core.core.init import connections
from flowmachine_core.query_bases.spatial_unit import make_spatial_unit
from flowmachine_core.query_bases.query import Query

logger = logging.getLogger()

here = os.path.dirname(os.path.abspath(__file__))
flowkit_toplevel_dir = Path(__file__).parent.parent.parent


class Nested(Query):
    def __init__(self, query_to_nest):
        self.query_to_nest = query_to_nest
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.query_to_nest.column_names

    def _make_query(self):
        return f"{self.query_to_nest.get_query()}"


@pytest.fixture
def test_query():
    return CustomQuery(sql="SELECT 1 as value", column_names=["value"])


@pytest.fixture
def nested_test_query(test_query):
    return Nested(test_query)


@pytest.fixture
def deeply_nested_test_query(nested_test_query):
    return Nested(nested_test_query)


@pytest.fixture
def json_log(caplog):
    def parse_json():
        loggers = dict(debug=[], query_run_log=[])
        for logger, level, msg in caplog.record_tuples:
            if msg == "":
                continue
            try:
                parsed = json.loads(msg)
                loggers[parsed["logger"].split(".")[1]].append(parsed)
            except JSONDecodeError:
                loggers["debug"].append(msg)
        return CaptureResult(err=loggers["debug"], out=loggers["query_run_log"])

    return parse_json


@pytest.fixture(
    params=[
        {"spatial_unit_type": "admin", "level": 2},
        {
            "spatial_unit_type": "admin",
            "level": 2,
            "region_id_column_name": "admin2name",
        },
        {"spatial_unit_type": "versioned-site"},
        {"spatial_unit_type": "versioned-cell"},
        {"spatial_unit_type": "cell"},
        {"spatial_unit_type": "lon-lat"},
        {"spatial_unit_type": "grid", "size": 5},
        {
            "spatial_unit_type": "polygon",
            "region_id_column_name": "admin3pcod",
            "geom_table": "geography.admin3",
        },
        {
            "spatial_unit_type": "polygon",
            "region_id_column_name": "id AS site_id",
            "geom_table": "infrastructure.sites",
            "geom_column": "geom_point",
        },
    ],
    ids=lambda x: str(x),
)
def exemplar_spatial_unit_param(request, flowmachine_connect):
    """
    A fixture which yields a succession of plausible values for the
    spatial_unit parameter.

    Yields
    ------
    flowmachine_core.core.spatial_unit.*SpatialUnit

    """
    yield make_spatial_unit(**request.param)


@pytest.fixture(autouse=True)
def flowmachine_connect():
    with connections():
        yield
        reset_cache(get_db(), get_redis(), protect_table_objects=False)
        get_db().engine.dispose()  # Close the connection
        get_redis().flushdb()  # Empty the redis


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
        Mocks for init_logging, Connection, StrictRedis and _start_threadpool

    """

    logging_mock = Mock()
    connection_mock = Mock()
    connection_mock.return_value.engine.begin.return_value.__enter__ = Mock()
    connection_mock.return_value.engine.begin.return_value.__exit__ = Mock()
    connection_mock.return_value.fetch.return_value = MagicMock(return_value=[])
    redis_mock = Mock(name="mocked_connections_redis")
    tp_mock = Mock(return_value=None)
    monkeypatch.setattr(flowmachine.core.init, "set_log_level", logging_mock)
    monkeypatch.setattr(flowmachine.core.init, "Connection", connection_mock)
    monkeypatch.setattr("redis.StrictRedis", redis_mock)
    monkeypatch.setattr(
        concurrent.futures.thread.ThreadPoolExecutor, "__init__", tp_mock
    )
    yield logging_mock, connection_mock, redis_mock, tp_mock


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
        con=get_db().engine,
    )


@pytest.fixture
def get_column_names_from_run(flowmachine_connect):
    yield lambda query: pd.read_sql_query(
        f"{query.get_query()} LIMIT 0;", con=get_db().engine
    ).columns.tolist()


@pytest.fixture
def get_length(flowmachine_connect):
    yield lambda query: len(pd.read_sql_query(query.get_query(), con=get_db().engine))


class DummyRedis:
    """
    Drop-in replacement for redis.
    """

    def __init__(self):
        self._store = {}
        self.allow_flush = True

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
        if (
            self.allow_flush
        ):  # Set allow_flush attribute to False to simulate concurrent writes
            self._store = {}


@pytest.fixture
def dummy_redis(flowmachine_connect):
    dummy_redis = DummyRedis()
    token = redis_connection.set(dummy_redis)
    print("Replaced redis with dummy redis.")
    yield dummy_redis
    redis_connection.reset(token)


@pytest.fixture(scope="session")
def diff_reporter():
    diff_reporter_factory = GenericDiffReporterFactory()
    try:
        with open(Path(__file__).parent / "reporters.json") as fin:
            for config in json.load(fin):
                diff_reporter_factory.add_default_reporter_config(config)
    except FileNotFoundError:
        pass
    differ = diff_reporter_factory.get_first_working()
    return partial(verify, reporter=differ)


@pytest.fixture
def redactable_locations():
    class Locatable(Query):
        def _make_query(self):
            return f"""SELECT * FROM (
                VALUES 
                ('a', 'a'),
                ('b', 'a'),
                ('c', 'a'),
                ('d', 'a'),
                ('e', 'a'),
                ('f', 'a'),
                ('g', 'a'),
                ('h', 'a'),
                ('i', 'a'),
                ('j', 'a'),
                ('k', 'a'),
                ('l', 'a'),
                ('m', 'a'),
                ('n', 'a'),
                ('o', 'a'),
                ('p', 'a'),
                ('q', 'a'),
                ('r', 'a'),
                ('s', 'a'),
                ('t', 'a'),
                ('u', 'b'),
                ('v', 'b'),
                ('w', 'b'),
                ('x', 'b'),
                ('y', 'b'),
                ('z', 'b')
                ) as t(subscriber, pcod)"""

        @property
        def column_names(self):
            return ["subscriber", "pcod"]

        spatial_unit = make_spatial_unit("admin", level=3)

    return Locatable()
