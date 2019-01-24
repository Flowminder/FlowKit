# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import pytest

from flowmachine import connect
from flowmachine.core import Query


@pytest.fixture(autouse=True)
def reset_connect(monkeypatch):
    monkeypatch.delattr("flowmachine.core.Query.connection")
    logging.getLogger("flowmachine").handlers = []


def test_double_connect_warning(monkeypatch):
    """Test that a warning is raised when connecting twice."""
    connect()
    with pytest.warns(UserWarning):
        connect()
    assert 1 == len(logging.getLogger("flowmachine").handlers)


def test_bad_log_level_goes_to_error(monkeypatch):
    """Test that a bad log level is coerced to ERROR."""
    monkeypatch.setenv("LOG_LEVEL", "BAD_LEVEL")
    connect()
    assert logging.ERROR == logging.getLogger("flowmachine").level


def test_log_level_set_env(monkeypatch):
    """Test that a log level can be set via env."""
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    connect()
    assert logging.INFO == logging.getLogger("flowmachine").level


def test_log_level_set(monkeypatch):
    """Test that a log level can be set via param."""

    connect(log_level="critical")
    assert logging.CRITICAL == logging.getLogger("flowmachine").level


def test_log_file_created(tmpdir, monkeypatch):
    """Test that a file handler is created when a path to LOG_FILE is set."""
    monkeypatch.setenv("LOG_FILE", str(tmpdir + "log_file"))
    connect()
    logging.getLogger("flowmachine").error("TEST_MESSAGE")
    with open(tmpdir + "log_file") as fin:
        log_lines = fin.readline()
    assert "TEST_MESSAGE" in log_lines
    assert 2 == len(logging.getLogger("flowmachine").handlers)


def test_param_priority(mocked_connections, monkeypatch):
    """Explicit parameters to connect should be respected"""
    # Use monkeypatch to set environment variable only for this test
    monkeypatch.setenv("LOG_LEVEL", "DUMMY_ENV_LOG_LEVEL")
    monkeypatch.setenv("LOG_FILE", "DUMMY_ENV_LOG_FILE")
    monkeypatch.setenv("DB_PORT", 7777)
    monkeypatch.setenv("DB_USER", "DUMMY_ENV_DB_USER")
    monkeypatch.setenv("DB_PW", "DUMMY_ENV_DB_PW")
    monkeypatch.setenv("DB_HOST", "DUMMY_ENV_DB_HOST")
    monkeypatch.setenv("DB_NAME", "DUMMY_ENV_DB_NAME")
    monkeypatch.setenv("POOL_SIZE", 7777)
    monkeypatch.setenv("POOL_OVERFLOW", 7777)
    monkeypatch.setenv("REDIS_HOST", "DUMMY_ENV_REDIS_HOST")
    monkeypatch.setenv("REDIS_PORT", 7777)
    monkeypatch.setenv("REDIS_PASSWORD", "DUMMY_ENV_REDIS_PASSWORD")
    core_init_logging_mock, core_init_Connection_mock, core_init_StrictRedis_mock, core_init_start_threadpool_mock = (
        mocked_connections
    )
    connect(
        log_level="dummy_log_level",
        log_file="dummy_log_file",
        db_port=1234,
        db_user="dummy_db_user",
        db_pw="dummy_db_pw",
        db_host="dummy_db_host",
        db_name="dummy_db_name",
        db_connection_pool_size=6789,
        db_connection_pool_overflow=1011,
        redis_host="dummy_redis_host",
        redis_port=1213,
        redis_password="dummy_redis_password",
    )
    core_init_logging_mock.assert_called_with("dummy_log_level", "dummy_log_file")
    core_init_Connection_mock.assert_called_with(
        1234,
        "dummy_db_user",
        "dummy_db_pw",
        "dummy_db_host",
        "dummy_db_name",
        6789,
        1011,
    )
    core_init_StrictRedis_mock.assert_called_with(
        host="dummy_redis_host", port=1213, password="dummy_redis_password"
    )


def test_env_priority(mocked_connections, monkeypatch):
    """Env vars should be used over defaults in connect"""
    # Use monkeypatch to set environment variable only for this test
    monkeypatch.setenv("LOG_LEVEL", "DUMMY_ENV_LOG_LEVEL")
    monkeypatch.setenv("LOG_FILE", "DUMMY_ENV_LOG_FILE")
    monkeypatch.setenv("DB_PORT", 6969)
    monkeypatch.setenv("DB_USER", "DUMMY_ENV_DB_USER")
    monkeypatch.setenv("DB_PW", "DUMMY_ENV_DB_PW")
    monkeypatch.setenv("DB_HOST", "DUMMY_ENV_DB_HOST")
    monkeypatch.setenv("DB_NAME", "DUMMY_ENV_DB_NAME")
    monkeypatch.setenv("POOL_SIZE", 7777)
    monkeypatch.setenv("POOL_OVERFLOW", 2020)
    monkeypatch.setenv("REDIS_HOST", "DUMMY_ENV_REDIS_HOST")
    monkeypatch.setenv("REDIS_PORT", 5050)
    monkeypatch.setenv("REDIS_PASSWORD", "DUMMY_ENV_REDIS_PASSWORD")
    core_init_logging_mock, core_init_Connection_mock, core_init_StrictRedis_mock, core_init_start_threadpool_mock = (
        mocked_connections
    )
    connect()
    core_init_logging_mock.assert_called_with(
        "DUMMY_ENV_LOG_LEVEL", "DUMMY_ENV_LOG_FILE"
    )
    core_init_Connection_mock.assert_called_with(
        6969,
        "DUMMY_ENV_DB_USER",
        "DUMMY_ENV_DB_PW",
        "DUMMY_ENV_DB_HOST",
        "DUMMY_ENV_DB_NAME",
        7777,
        2020,
    )
    core_init_StrictRedis_mock.assert_called_with(
        host="DUMMY_ENV_REDIS_HOST", port=5050, password="DUMMY_ENV_REDIS_PASSWORD"
    )


@pytest.mark.usefixtures("clean_env")
def test_connect_defaults(mocked_connections, monkeypatch):
    """Test connect defaults are used with no params and no env vars"""
    core_init_logging_mock, core_init_Connection_mock, core_init_StrictRedis_mock, core_init_start_threadpool_mock = (
        mocked_connections
    )
    connect()
    core_init_logging_mock.assert_called_with("error", False)
    core_init_Connection_mock.assert_called_with(
        9000, "analyst", "foo", "localhost", "flowdb", 5, 1
    )
    core_init_StrictRedis_mock.assert_called_with(
        host="localhost", port=6379, password="fm_redis"
    )
