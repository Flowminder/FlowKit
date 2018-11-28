# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Test column_names property of  _TotalCellEvents
"""

import pytest

from flowmachine.features.location.total_events import (
    TotalLocationEvents,
    _TotalCellEvents,
)


@pytest.mark.usefixtures("skip_datecheck")
@pytest.mark.parametrize("interval", TotalLocationEvents.allowed_levels)
@pytest.mark.parametrize("direction", ["in", "out", "both"])
def test_total_cell_events_column_names(interval, direction):
    """ Test that column_names property of _TotalCellEvents matches head(0)"""
    tce = _TotalCellEvents(
        "2016-01-01", "2016-01-04", interval=interval, direction=direction
    )
    assert tce.head(0).columns.tolist() == tce.column_names
