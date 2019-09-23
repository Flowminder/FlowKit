# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.server.server_config import get_server_config


def test_get_server_config(monkeypatch):
    """
    Test that get_server_config correctly reads config options from environment variables.
    """
    monkeypatch.setenv("FLOWMACHINE_PORT", 5678)
    monkeypatch.setenv("FLOWMACHINE_SERVER_DEBUG_MODE", "true")
    monkeypatch.setenv("FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING", "true")
    config = get_server_config()
    assert config.port == 5678
    assert config.debug_mode == True
    assert config.store_dependencies == False


def test_get_server_config_defaults(monkeypatch):
    """
    Test that get_server_config returns default config settings if environment variables are not set.
    """
    monkeypatch.delenv("FLOWMACHINE_PORT", raising=False)
    monkeypatch.delenv("FLOWMACHINE_SERVER_DEBUG_MODE", raising=False)
    monkeypatch.delenv("FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING", raising=False)
    config = get_server_config()
    assert config.port == 5555
    assert config.debug_mode == False
    assert config.store_dependencies == True
