# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import unittest
from pathlib import Path

from app.main import getsecret


def test_get_secrets(monkeypatch):
    """Test getting a secret from the special /run/secrets directory."""
    the_secret = "Shhhh"
    the_secret_name = "SECRET"
    open_mock = unittest.mock.mock_open(read_data=the_secret)
    monkeypatch.setattr("builtins.open", open_mock)
    secret = getsecret(the_secret_name, "Not the secret")
    assert the_secret == secret
    open_mock.assert_called_once_with(Path("/run/secrets") / the_secret_name, "r")


def test_get_secrets_default(monkeypatch):
    """Test getting a secret falls back to provided default with the file being there."""
    the_secret = "Shhhh"
    the_secret_name = "SECRET"
    secret = getsecret(the_secret_name, the_secret)
    assert the_secret == secret
