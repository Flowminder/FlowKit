# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features import LastLocation, SubscriberLocations, UniqueLocations
from flowmachine.features.location.redacted_unmoving_at_reference_location_counts import (
    RedactedUnmovingAtReferenceLocationCounts,
)
from flowmachine.features.location.unmoving_at_reference_location_counts import (
    UnmovingAtReferenceLocationCounts,
)
from flowmachine.features.subscriber.unmoving_at_reference_location import (
    UnmovingAtReferenceLocation,
)


def test_unmoving_at_reference_location_counts_column_names(get_column_names_from_run):
    assert (
        get_column_names_from_run(
            RedactedUnmovingAtReferenceLocationCounts(
                unmoving_at_reference_location_counts=UnmovingAtReferenceLocationCounts(
                    UnmovingAtReferenceLocation(
                        locations=UniqueLocations(
                            SubscriberLocations(
                                "2016-01-01",
                                "2016-01-01 10:00",
                                spatial_unit=make_spatial_unit("admin", level=1),
                            )
                        ),
                        reference_locations=LastLocation(
                            "2016-01-01",
                            "2016-01-02",
                            spatial_unit=make_spatial_unit("admin", level=1),
                        ),
                    )
                )
            )
        )
        == ["pcod", "value"]
    )


def test_unmoving_at_reference_location_counts_values(get_dataframe):
    df = get_dataframe(
        RedactedUnmovingAtReferenceLocationCounts(
            unmoving_at_reference_location_counts=UnmovingAtReferenceLocationCounts(
                UnmovingAtReferenceLocation(
                    locations=UniqueLocations(
                        SubscriberLocations(
                            "2016-01-01",
                            "2016-01-01 10:00",
                            spatial_unit=make_spatial_unit("admin", level=1),
                        )
                    ),
                    reference_locations=LastLocation(
                        "2016-01-01",
                        "2016-01-02",
                        spatial_unit=make_spatial_unit("admin", level=1),
                    ),
                )
            )
        )
    ).set_index("pcod")
    with pytest.raises(KeyError):
        df.loc["524 1"].value == 2
    assert df.loc["524 4"].value == 26
