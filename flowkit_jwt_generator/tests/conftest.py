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
    the expected decoded user claims dict.
    """
    get_mock = Mock()
    get_mock.return_value.json.return_value = dict(
        components=dict(
            securitySchemes=dict(
                token={
                    "x-security-scopes": ["get_result&DUMMY_QUERY_KIND.admin0"],
                    "x-audience": "DUMMY",
                }
            )
        )
    )

    monkeypatch.setattr("requests.get", get_mock)
    return dict(claims=["get_result&DUMMY_QUERY_KIND.admin0"], aud="DUMMY")
