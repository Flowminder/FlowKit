# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import make_spatial_unit
from flowmachine.core.context import get_db
from flowmachine.features.subscriber import *


def test_default_indexes():
    """
    Check that default indexing columns are correct
    """
    assert daily_location("2016-01-01", "2016-01-02").index_cols == [
        ["pcod"],
        '"subscriber"',
    ]
    assert daily_location(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("lon-lat")
    ).index_cols == [["lon", "lat"], '"subscriber"']
    assert SubscriberDegree("2016-01-01", "2016-01-02").index_cols == ['"subscriber"']


def test_index_created(flowmachine_connect):
    """
    Check that an index actually gets created on storage.
    """
    dl = daily_location("2016-01-01", "2016-01-02")
    dl.store().result()
    ix_qur = "SELECT * FROM pg_indexes WHERE tablename='{}'".format(
        dl.fully_qualified_table_name.split(".")[1]
    )
    assert len(get_db().fetch(ix_qur)) == 2
