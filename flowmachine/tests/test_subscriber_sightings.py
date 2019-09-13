# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.features import SubscriberSigntings


@pytest.mark.parametrize("identifier", ("msisdn", "imei", "imsi"))
def test_colums_are_set(identifier):
    """Add a test to test something."""
    ss = SubscriberSigntings(
        "2016-01-01", "2016-01-02", subscriber_identifier=identifier
    )

    # TODO - check the columns

    assert True == True
