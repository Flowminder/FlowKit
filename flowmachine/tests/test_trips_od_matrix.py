# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.features import SubscriberLocations
from flowmachine.features.location.trips_od_matrix import TripsODMatrix


def test_trips_column_names(get_column_names_from_run):
    assert get_column_names_from_run(
        TripsODMatrix(
            subscriber_locations=SubscriberLocations(
                "2016-01-01 13:30:30", "2016-01-07 16:25:00"
            )
        )
    ) == ["location_id_from", "location_id_to", "value"]


def test_trips_counts(get_dataframe):
    """
    Values test for  trips counts.
    """
    trips = TripsODMatrix(
        subscriber_locations=SubscriberLocations(
            "2016-01-01 13:30:30", "2016-01-07 16:25:00"
        )
    )
    df = get_dataframe(trips).set_index(["location_id_from", "location_id_to"])
    assert df.loc["0RIMKYtf", "ZOsVSeQS"].value == 162
    assert df.loc["0RIMKYtf", "YrCNbyNK"].value == 59
