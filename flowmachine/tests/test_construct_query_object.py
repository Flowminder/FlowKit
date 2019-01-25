# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.server.query_proxy import (
    construct_query_object,
    InvalidGeographyError,
)


@pytest.mark.parametrize(
    "expected_md5, query_spec",
    [
        (
            "5b2b2484e941da429b52dda7e81fb917",
            {"query_kind": "geography", "params": {"aggregation_unit": "admin3"}},
        )
    ],
)
def test_construct_query(expected_md5, query_spec):
    """
    Test that expected query objects are constructed by construct_query_object
    """
    obj = construct_query_object(**query_spec)
    assert expected_md5 == obj.md5


def test_wrong_geography_aggregation_unit_raises_error():
    with pytest.raises(
        InvalidGeographyError,
        match="Unrecognised aggregation unit 'DUMMY_AGGREGATION_UNIT'",
    ):
        obj = construct_query_object(
            "geography", {"aggregation_unit": "DUMMY_AGGREGATION_UNIT"}
        )
