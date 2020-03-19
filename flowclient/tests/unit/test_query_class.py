# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from unittest.mock import Mock

from flowclient.query import Query


def test_query_run():
    connection_mock = Mock()
    connection_mock.post_json.return_value = Mock(
        status_code=202, headers={"Location": "DUMMY_LOCATION/DUMMY_ID"}
    )
    query_spec = {"query_kind": "dummy_query"}
    query = Query(connection=connection_mock, parameters=query_spec)
    assert not hasattr(query, "_query_id")
    query.run()
    connection_mock.post_json.assert_called_once_with(route="run", data=query_spec)
    assert query._query_id == "DUMMY_ID"

