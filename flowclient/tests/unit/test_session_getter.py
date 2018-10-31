# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from _ssl import SSLError
from unittest.mock import Mock

import requests
from hyper.contrib import HTTP20Adapter

from flowclient.client import _get_session


def test_getting_http1_session(monkeypatch):
    """ Test that without hyper, a http1 session is created. """
    monkeypatch.delattr("flowclient.client.HTTP20Adapter")  # Disable hyper
    session = _get_session("DUMMY_URL", "PRETEND_CERTIFICATE")
    assert isinstance(session, requests.Session)
    assert "PRETEND_CERTIFICATE" == session.verify
    assert (
        "DUMMY_URL" not in session.adapters
    )  # Check no special adapter has been added


def test_getting_http1_session_fallback(monkeypatch):
    """ Test that if a valid ssl cert isn't available at the endpoint, client falls back to http1. """
    session_mock = Mock(side_effect=SSLError)
    monkeypatch.setattr("requests.Session.get", session_mock)
    session = _get_session("DUMMY_URL", "PRETEND_CERTIFICATE")
    assert isinstance(session, requests.Session)
    assert "PRETEND_CERTIFICATE" == session.verify
    assert (
        "DUMMY_URL" not in session.adapters
    )  # Check no special adapter has been added


def test_getting_http2_session(monkeypatch):
    """ Test that with hyper, and a compliant endpoint, an http2 session is created"""
    session_mock = Mock()
    monkeypatch.setattr("requests.Session.get", session_mock)
    session = _get_session("DUMMY_URL", None)
    assert isinstance(session, requests.Session)  # Should have a valid session object
    assert session.verify  # Certificate verification should be enabled
    assert (
        "DUMMY_URL" in session.adapters
    )  # Hyper should be registered to handle interaction with the url
    assert isinstance(session.adapters["DUMMY_URL"], HTTP20Adapter)
