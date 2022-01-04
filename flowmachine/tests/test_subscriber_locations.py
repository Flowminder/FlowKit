# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations

pytestmark = pytest.mark.usefixtures("skip_datecheck")


@pytest.mark.parametrize("ignore_nulls", [True, False])
def test_column_names(exemplar_spatial_unit_param, ignore_nulls):
    """Test that column_names property matches head(0) for SubscriberLocations"""
    sl = SubscriberLocations(
        "2016-01-01",
        "2016-01-04",
        spatial_unit=exemplar_spatial_unit_param,
        ignore_nulls=ignore_nulls,
    )
    assert sl.head(0).columns.tolist() == sl.column_names


def test_can_get_pcods(get_dataframe):
    """
    SubscriberLocations() can make queries at the p-code level.
    """

    subscriber_pcod = SubscriberLocations(
        "2016-01-01 13:30:30",
        "2016-01-02 16:25:00",
        spatial_unit=make_spatial_unit(
            "polygon", region_id_column_name="admin3pcod", geom_table="geography.admin3"
        ),
    )
    df = get_dataframe(subscriber_pcod)
    assert df.admin3pcod[0].startswith("524")
