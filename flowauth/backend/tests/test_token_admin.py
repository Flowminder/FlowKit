# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from cryptography.hazmat.primitives import serialization
from os import environ
from flowkit_jwt_generator import load_public_key

import pytest

#
# @pytest.mark.usefixtures("test_data")
# def test_list_tokens_for_server(client, auth, test_admin):
#     uid, uname, upass = test_admin
#     # Log in first
#     response, csrf_cookie = auth.login(uname, upass)
#     response = client.get("/admin/tokens", headers={"X-CSRF-Token": csrf_cookie})
#     assert 200 == response.status_code
#     assert 1 == len(response.get_json())


@pytest.mark.usefixtures("test_data")
def test_get_server_public_key(app, client, auth, public_key_bytes):
    """Test that admin can get the flowauth server's public key"""
    response, csrf_cookie = auth.login("TEST_ADMIN", "DUMMY_PASSWORD")

    response = client.get("/admin/public_key", headers={"X-CSRF-Token": csrf_cookie})
    key_json = response.get_json()
    assert public_key_bytes == load_public_key(key_json["public_key"]).public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
