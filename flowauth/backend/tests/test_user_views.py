# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


@pytest.mark.skip(reason="Users do not have direct access to servers anymore")
@pytest.mark.usefixtures("test_data")
def test_server_access(client, auth, test_user):
    uid, uname, upass = test_user
    # Log in first
    response, csrf_cookie = auth.login(uname, upass)
    response = client.get("/tokens/servers", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code  # Should get an OK
    assert [{"id": 1, "server_name": "DUMMY_SERVER_A"}] == response.get_json()


# Replaece with test_no_roles
# @pytest.mark.usefixtures("test_data")
# def test_no_tokens(client, auth, test_user):
#     uid, uname, upass = test_user
#     Log in first
# response, csrf_cookie = auth.login(uname, upass)
# response = client.get("/tokens/tokens", headers={"X-CSRF-Token": csrf_cookie})
# assert 200 == response.status_code  # Should get an OK
# result = response.get_json()
#
# assert [] == result  # Can't do anything
