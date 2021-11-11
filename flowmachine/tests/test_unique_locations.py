# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core import make_spatial_unit
from flowmachine.features import SubscriberLocations
from flowmachine.features.subscriber.unique_locations import UniqueLocations


def test_unique_locations_column_names(get_column_names_from_run):
    assert get_column_names_from_run(
        UniqueLocations(
            SubscriberLocations(
                "2016-01-01",
                "2016-01-02",
                spatial_unit=make_spatial_unit("admin", level=3),
            )
        )
    ) == ["subscriber", "pcod"]


def test_unique_locations(get_dataframe):
    """
    Values test for unique locations.
    """
    unique_locs = UniqueLocations(
        SubscriberLocations(
            "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("admin", level=3)
        )
    )
    df = get_dataframe(unique_locs).set_index("subscriber")
    assert df.loc["038OVABN11Ak4W5P"].pcod.tolist() == [
        "524 2 04 20",
        "524 3 08 43",
        "524 4 12 62",
        "524 4 12 65",
    ]
