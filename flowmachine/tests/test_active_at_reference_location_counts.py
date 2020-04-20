# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core import make_spatial_unit
from flowmachine.features import SubscriberLocations, daily_location
from flowmachine.features.location.active_at_reference_location_counts import (
    ActiveAtReferenceLocationCounts,
)
from flowmachine.features.subscriber.active_at_reference_location import (
    ActiveAtReferenceLocation,
)
from flowmachine.features.subscriber.unique_locations import UniqueLocations


def test_active_at_reference_location_counts_column_names(get_column_names_from_run):
    assert get_column_names_from_run(
        ActiveAtReferenceLocationCounts(
            ActiveAtReferenceLocation(
                subscriber_locations=UniqueLocations(
                    SubscriberLocations(
                        "2016-01-01",
                        "2016-01-02",
                        spatial_unit=make_spatial_unit("admin", level=3),
                    )
                ),
                reference_locations=daily_location("2016-01-03"),
            )
        )
    ) == ["pcod", "value"]


def test_active_at_reference_location_counts(get_dataframe):
    """
    Values test for active at reference location counts.
    """
    activity = ActiveAtReferenceLocationCounts(
        ActiveAtReferenceLocation(
            subscriber_locations=UniqueLocations(
                SubscriberLocations(
                    "2016-01-01",
                    "2016-01-02",
                    spatial_unit=make_spatial_unit("admin", level=3),
                )
            ),
            reference_locations=daily_location("2016-01-03"),
        )
    )
    df = get_dataframe(activity).set_index("pcod")
    assert df.loc["524 1 01 04"][0] == 1
