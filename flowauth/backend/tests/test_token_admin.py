# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


@pytest.mark.usefixtures("test_data")
def test_list_tokens_for_server(client, auth):
    # Log in first
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")
    response = client.get("/admin/tokens", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code
    assert 1 == len(response.get_json())


@pytest.mark.usefixtures("test_data")
def test_get_server_public_key(app, client, auth):
    """Test that admin can get the flowauth server's public key"""
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

    response = client.get("/admin/public_key", headers={"X-CSRF-Token": csrf_cookie})
    assert app.config["PUBLIC_JWT_SIGNING_KEY"] == response.data
