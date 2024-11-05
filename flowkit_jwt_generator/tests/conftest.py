# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

import pytest

pytest_plugins = ["pytester"]


@pytest.fixture
def dummy_flowapi(monkeypatch):
    """
    Fixture which monkeypatches requests.get to return a dummy flowapi schema and returns
    the universal role for that dummy flowapi schema
    """
    get_mock = Mock()
    get_mock.return_value.json.return_value = dict(
        components=dict(
            securitySchemes=dict(
                token={
                    "x-security-scopes": [
                        "dummy_scope",
                        "dummy_admin:dummy_query:dummy_query",
                    ],
                    "x-audience": "DUMMY",
                }
            )
        )
    )

    monkeypatch.setattr("requests.get", get_mock)
    return dict(
        roles=dict(
            universal_role=["dummy_scope", "dummy_admin:dummy_query:dummy_query"]
        ),
        aud="DUMMY",
    )
