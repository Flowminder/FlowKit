# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core import make_spatial_unit
from flowmachine.features import SubscriberLocations, daily_location
from flowmachine.features.subscriber.active_at_reference_location import (
    ActiveAtReferenceLocation,
)
from flowmachine.features.subscriber.unique_locations import UniqueLocations


def test_active_at_reference_location_column_names(get_column_names_from_run):
    assert get_column_names_from_run(
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
    ) == ["subscriber", "value"]


def test_active_at_reference_location(get_dataframe):
    """
    Values test for active at reference location.
    """
    activity = ActiveAtReferenceLocation(
        subscriber_locations=UniqueLocations(
            SubscriberLocations(
                "2016-01-01",
                "2016-01-02",
                spatial_unit=make_spatial_unit("admin", level=3),
            )
        ),
        reference_locations=daily_location("2016-01-03"),
    )
    df = get_dataframe(activity).set_index("subscriber")
    assert not df.loc["038OVABN11Ak4W5P"][0]
    assert df.loc["09NrjaNNvDanD8pk"][0]
