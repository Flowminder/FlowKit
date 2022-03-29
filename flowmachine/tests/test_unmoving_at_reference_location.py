# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features import LastLocation, SubscriberLocations, UniqueLocations
from flowmachine.features.subscriber.unmoving_at_reference_location import (
    UnmovingAtReferenceLocation,
)


def test_unmoving_at_reference_location_column_names(get_column_names_from_run):
    assert get_column_names_from_run(
        UnmovingAtReferenceLocation(
            locations=UniqueLocations(
                SubscriberLocations(
                    "2016-01-01",
                    "2016-01-01 10:00",
                    spatial_unit=make_spatial_unit("admin", level=3),
                )
            ),
            reference_locations=LastLocation("2016-01-01", "2016-01-02"),
        )
    ) == ["subscriber", "value"]


def test_spatial_unit_mismatch_error():
    with pytest.raises(ValueError, match="Spatial unit mismatch"):
        UnmovingAtReferenceLocation(
            locations=UniqueLocations(
                SubscriberLocations(
                    "2016-01-01",
                    "2016-01-01 10:00",
                    spatial_unit=make_spatial_unit("admin", level=2),
                )
            ),
            reference_locations=LastLocation(
                "2016-01-01",
                "2016-01-02",
                spatial_unit=make_spatial_unit("admin", level=3),
            ),
        )


def test_unmoving_at_reference_location_values(get_dataframe):
    df = get_dataframe(
        UnmovingAtReferenceLocation(
            locations=UniqueLocations(
                SubscriberLocations(
                    "2016-01-01",
                    "2016-01-01 10:00",
                    spatial_unit=make_spatial_unit("admin", level=3),
                )
            ),
            reference_locations=LastLocation("2016-01-01", "2016-01-02"),
        )
    ).set_index("subscriber")
    assert not df.loc["038OVABN11Ak4W5P"].value
    assert df.loc["3XKdxqvyNxO2vLD1"].value
