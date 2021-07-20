# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import pytest

from flowmachine.core import connect
from flowmachine.core.context import get_interpreter_id, get_executor


def test_consistent_interpreter_id():
    connect()
    assert get_executor().submit(get_interpreter_id).result() == get_interpreter_id()


@pytest.fixture(autouse=True)
def reset_connect(monkeypatch):
    logging.getLogger("flowmachine.debug").handlers = []


def test_double_connect_warning():
    """Test that a warning is raised when connecting twice."""
    connect()
    with pytest.warns(UserWarning):
        connect()
    # assert 1 == len(logging.getLogger("flowmachine.debug").handlers)


def test_bad_log_level_goes_to_error(monkeypatch):
    """Test that a bad log level is coerced to ERROR."""
    monkeypatch.setenv("FLOWMACHINE_LOG_LEVEL", "BAD_LEVEL")
    connect()
    assert logging.ERROR == logging.getLogger("flowmachine.debug").level


def test_log_level_set_env(monkeypatch):
    """Test that a log level can be set via env."""
    monkeypatch.setenv("FLOWMACHINE_LOG_LEVEL", "INFO")
    connect()
    assert logging.INFO == logging.getLogger("flowmachine.debug").level


def test_log_level_set(monkeypatch):
    """Test that a log level can be set via param."""

    connect(log_level="critical")
    assert logging.CRITICAL == logging.getLogger("flowmachine.debug").level


def test_param_priority(mocked_connections, monkeypatch):
    """Explicit parameters to connect should be respected"""
    # Use monkeypatch to set environment variable only for this test
    monkeypatch.setenv("FLOWMACHINE_LOG_LEVEL", "DUMMY_ENV_LOG_LEVEL")
    monkeypatch.setenv("FLOWDB_PORT", "7777")
    monkeypatch.setenv("FLOWMACHINE_FLOWDB_USER", "DUMMY_ENV_FLOWDB_USER")
    monkeypatch.setenv("FLOWDB_PASSWORD", "DUMMY_ENV_FLOWDB_PASSWORD")
    monkeypatch.setenv("FLOWDB_HOST", "DUMMY_ENV_FLOWDB_HOST")
    monkeypatch.setenv("DB_CONNECTION_POOL_SIZE", "7777")
    monkeypatch.setenv("DB_CONNECTION_POOL_OVERFLOW", "7777")
    monkeypatch.setenv("REDIS_HOST", "DUMMY_ENV_REDIS_HOST")
    monkeypatch.setenv("REDIS_PORT", "7777")
    monkeypatch.setenv("REDIS_PASSWORD", "DUMMY_ENV_REDIS_PASSWORD")
    (
        core_set_log_level_mock,
        core_init_Connection_mock,
        core_init_StrictRedis_mock,
        core_init_start_threadpool_mock,
    ) = mocked_connections
    connect(
        log_level="dummy_log_level",
        flowdb_port=1234,
        flowdb_user="dummy_db_user",
        flowdb_password="dummy_db_pass",
        flowdb_host="dummy_db_host",
        flowdb_connection_pool_size=6789,
        flowdb_connection_pool_overflow=1011,
        redis_host="dummy_redis_host",
        redis_port=1213,
        redis_password="dummy_redis_password",
    )
    core_set_log_level_mock.assert_called_with("flowmachine.debug", "dummy_log_level")
    core_init_Connection_mock.assert_called_with(
        port=1234,
        user="dummy_db_user",
        password="dummy_db_pass",
        host="dummy_db_host",
        database="flowdb",
        pool_size=6789,
        overflow=1011,
    )
    core_init_StrictRedis_mock.assert_called_with(
        host="dummy_redis_host", port=1213, password="dummy_redis_password"
    )
    core_init_start_threadpool_mock.assert_called_with(
        6789
    )  # for the time being, we should have num_threads = num_db_connections


def test_env_priority(mocked_connections, monkeypatch):
    """Env vars should be used over defaults in connect"""
    # Use monkeypatch to set environment variable only for this test
    monkeypatch.setenv("FLOWMACHINE_LOG_LEVEL", "DUMMY_ENV_LOG_LEVEL")
    monkeypatch.setenv("FLOWDB_PORT", "6969")
    monkeypatch.setenv("FLOWMACHINE_FLOWDB_USER", "DUMMY_ENV_FLOWDB_USER")
    monkeypatch.setenv("FLOWMACHINE_FLOWDB_PASSWORD", "DUMMY_ENV_FLOWDB_PASSWORD")
    monkeypatch.setenv("FLOWDB_HOST", "DUMMY_ENV_FLOWDB_HOST")
    monkeypatch.setenv("DB_CONNECTION_POOL_SIZE", "7777")
    monkeypatch.setenv("DB_CONNECTION_POOL_OVERFLOW", "2020")
    monkeypatch.setenv("REDIS_HOST", "DUMMY_ENV_REDIS_HOST")
    monkeypatch.setenv("REDIS_PORT", "5050")
    monkeypatch.setenv("REDIS_PASSWORD", "DUMMY_ENV_REDIS_PASSWORD")
    (
        core_set_log_level_mock,
        core_init_Connection_mock,
        core_init_StrictRedis_mock,
        core_init_start_threadpool_mock,
    ) = mocked_connections
    connect()
    core_set_log_level_mock.assert_called_with(
        "flowmachine.debug", "DUMMY_ENV_LOG_LEVEL"
    )
    core_init_Connection_mock.assert_called_with(
        port=6969,
        user="DUMMY_ENV_FLOWDB_USER",
        password="DUMMY_ENV_FLOWDB_PASSWORD",
        host="DUMMY_ENV_FLOWDB_HOST",
        database="flowdb",
        pool_size=7777,
        overflow=2020,
    )
    core_init_StrictRedis_mock.assert_called_with(
        host="DUMMY_ENV_REDIS_HOST", port=5050, password="DUMMY_ENV_REDIS_PASSWORD"
    )
    core_init_start_threadpool_mock.assert_called_with(
        7777
    )  # for the time being, we should have num_threads = num_db_connections


@pytest.mark.usefixtures("clean_env")
def test_connect_defaults(mocked_connections, monkeypatch):
    """Test connect defaults are used with no params and no env vars"""
    (
        core_set_log_level_mock,
        core_init_Connection_mock,
        core_init_StrictRedis_mock,
        core_init_start_threadpool_mock,
    ) = mocked_connections
    connect(flowdb_password="foo", redis_password="fm_redis")
    core_set_log_level_mock.assert_called_with("flowmachine.debug", "error")
    core_init_Connection_mock.assert_called_with(
        port=9000,
        user="flowmachine",
        password="foo",
        host="localhost",
        database="flowdb",
        pool_size=5,
        overflow=1,
    )
    core_init_StrictRedis_mock.assert_called_with(
        host="localhost", port=6379, password="fm_redis"
    )
    core_init_start_threadpool_mock.assert_called_with(
        5
    )  # for the time being, we should have num_threads = num_db_connections


@pytest.mark.usefixtures("clean_env")
@pytest.mark.parametrize(
    "args", [{}, {"flowdb_password": "foo"}, {"redis_password": "fm_redis"}]
)
def test_connect_passwords_required(args):
    """Test connect raises a valueerror if no password is set for db or redis"""
    with pytest.raises(ValueError):
        connect(**args)
