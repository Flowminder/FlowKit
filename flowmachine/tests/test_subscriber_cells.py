# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.utilities.subscriber_locations import _SubscriberCells

import pytest

pytestmark = pytest.mark.usefixtures("skip_datecheck")


@pytest.mark.parametrize("ignore_nulls", [True, False])
def test_column_names(ignore_nulls):
    """ Test that column_names property matches head(0) for _SubscriberCells"""
    sc = _SubscriberCells("2016-01-01", "2016-01-04", ignore_nulls=ignore_nulls)
    assert sc.head(0).columns.tolist() == sc.column_names
