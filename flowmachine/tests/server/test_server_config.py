# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os

import pytest

from flowmachine.core.server.server_config import get_server_config, get_env_as_bool


@pytest.mark.parametrize(
    "env_value, expected",
    [
        ("True", True),
        ("TRUE", True),
        ("true", True),
        ("", False),
        (1, False),
        ("False", False),
        (True, True),
        (False, False),
    ],
)
def test_bool_env(env_value, expected, monkeypatch):
    """
    Test getting env vars as bools.
    """
    monkeypatch.setenv("DUMMY_ENV_VAR", env_value)
    assert get_env_as_bool("DUMMY_ENV_VAR") == expected


def test_get_server_config(monkeypatch):
    """
    Test that get_server_config correctly reads config options from environment variables.
    """
    monkeypatch.setenv("FLOWMACHINE_PORT", 5678)
    monkeypatch.setenv("FLOWMACHINE_SERVER_DEBUG_MODE", "true")
    monkeypatch.setenv("FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING", "true")
    monkeypatch.setenv("FLOWMACHINE_CACHE_PRUNING_FREQUENCY", 1)
    monkeypatch.setenv("FLOWMACHINE_CACHE_PRUNING_TIMEOUT", 2)
    monkeypatch.setenv("FLOWMACHINE_SERVER_THREADPOOL_SIZE", 1)
    config = get_server_config()
    assert len(config) == 6
    assert config.port == 5678
    assert config.debug_mode
    assert not config.store_dependencies
    assert config.cache_pruning_timeout == 2
    assert config.cache_pruning_frequency == 1
    assert config.server_thread_pool._max_workers == 1


def test_get_server_config_defaults(monkeypatch):
    """
    Test that get_server_config returns default config settings if environment variables are not set.
    """
    monkeypatch.delenv("FLOWMACHINE_PORT", raising=False)
    monkeypatch.delenv("FLOWMACHINE_SERVER_DEBUG_MODE", raising=False)
    monkeypatch.delenv("FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING", raising=False)
    monkeypatch.delenv("FLOWMACHINE_CACHE_PRUNING_FREQUENCY", raising=False)
    monkeypatch.delenv("FLOWMACHINE_CACHE_PRUNING_TIMEOUT", raising=False)
    monkeypatch.delenv("FLOWMACHINE_SERVER_THREADPOOL_SIZE", raising=False)
    config = get_server_config()
    assert len(config) == 6
    assert config.port == 5555
    assert not config.debug_mode
    assert config.store_dependencies
    assert config.cache_pruning_timeout == 600
    assert config.cache_pruning_frequency == 86400
    assert config.server_thread_pool._max_workers == min(32, os.cpu_count() + 4)
