# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import unittest.mock
from pathlib import Path

from flowapi.config import get_secret_or_env_var, UndefinedConfigOption


def test_get_secrets(monkeypatch):
    """
    Test getting a secret from the special /run/secrets directory.
    """
    the_secret_name = "SECRET"
    the_secret = "Shhhh"
    open_mock = unittest.mock.mock_open(read_data=the_secret)
    monkeypatch.setattr("builtins.open", open_mock)
    secret = get_secret_or_env_var(the_secret_name)
    assert the_secret == secret
    open_mock.assert_called_once_with(Path("/run/secrets") / the_secret_name, "r")


def test_get_secrets_with_env_var(monkeypatch):
    """
    Test getting a secret falls back to the environment variable if the secret is undefined.
    """
    monkeypatch.setenv("SECRET", "Hush")
    secret = get_secret_or_env_var("SECRET")
    assert "Hush" == secret


def test_get_secrets_raises_error_if_value_is_undefined(monkeypatch):
    """
    Test getting a secret raises an error if neither the secret nor the env var is defined.
    """
    with pytest.raises(UndefinedConfigOption, match=""):
        get_secret_or_env_var("SECRET")
