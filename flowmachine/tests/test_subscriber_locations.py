# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.core.spatial_unit import make_spatial_unit
from flowmachine.features.utilities.subscriber_locations import subscriber_locations


def test_can_get_pcods(get_dataframe):
    """
    subscriber_locations() can make queries at the p-code level.
    """

    subscriber_pcod = subscriber_locations(
        "2016-01-01 13:30:30",
        "2016-01-02 16:25:00",
        spatial_unit=make_spatial_unit(
            "polygon", region_id_column_name="admin3pcod", geom_table="geography.admin3"
        ),
    )
    df = get_dataframe(subscriber_pcod)
    assert df.admin3pcod[0].startswith("524")
