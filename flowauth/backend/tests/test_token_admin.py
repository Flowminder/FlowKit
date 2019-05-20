# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest


@pytest.mark.usefixtures("test_data")
def test_list_tokens_for_server(client, auth, test_admin):
    uid, uname, upass = test_admin
    # Log in first
    response, csrf_cookie = auth.login(uname, upass)
    response = client.get("/admin/tokens", headers={"X-CSRF-Token": csrf_cookie})
    assert 200 == response.status_code
    assert 1 == len(response.get_json())
