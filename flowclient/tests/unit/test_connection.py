# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import jwt
import pytest

import flowclient
from flowclient.errors import FlowclientConnectionError


def test_update_token(session_mock, token):
    """
    Test that update_token updates all attributes appropriately.
    """
    connection = flowclient.connect(url="DUMMY_API", token=token)
    new_token = jwt.encode({"identity": "NEW_IDENTITY"}, "secret")
    connection.update_token(new_token)
    assert connection.user == "NEW_IDENTITY"
    assert connection.token == new_token
    assert session_mock.headers["Authorization"] == f"Bearer {new_token}"


@pytest.mark.parametrize("new_token", ["NOT_A_TOKEN", jwt.encode({}, "secret")])
def test_update_token_raises(session_mock, token, new_token):
    """
    Test that update_token raises an error if the token is not valid or does not contain an identity.
    """
    connection = flowclient.connect(url="DUMMY_API", token=token)
    with pytest.raises(FlowclientConnectionError):
        connection.update_token(new_token)
