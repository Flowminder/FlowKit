# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import flowauth


def test_version(client, app):
    """Test the correct version is returned."""
    response = client.get("/version")
    assert response.status_code == 200  # Should get an OK

    assert response.get_json() == {"version": flowauth.__version__}
