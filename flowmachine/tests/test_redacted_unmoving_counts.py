# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features import UniqueLocations, SubscriberLocations
from flowmachine.features.subscriber.unmoving import Unmoving
from flowmachine.features.location.unmoving_counts import UnmovingCounts
from flowmachine.features.location.redacted_unmoving_counts import (
    RedactedUnmovingCounts,
)


def test_unmoving_counts_column_names(get_column_names_from_run):
    assert get_column_names_from_run(
        RedactedUnmovingCounts(
            unmoving_counts=UnmovingCounts(
                Unmoving(
                    locations=UniqueLocations(
                        SubscriberLocations(
                            "2016-01-01",
                            "2016-01-01 10:00",
                            spatial_unit=make_spatial_unit("admin", level=3),
                        )
                    )
                )
            )
        )
    ) == ["pcod", "value"]


def test_unmoving_counts_values(get_dataframe):
    df = get_dataframe(
        RedactedUnmovingCounts(
            unmoving_counts=UnmovingCounts(
                Unmoving(
                    locations=UniqueLocations(
                        SubscriberLocations(
                            "2016-01-01",
                            "2016-01-01 10:00",
                            spatial_unit=make_spatial_unit("admin", level=2),
                        )
                    )
                )
            )
        )
    ).set_index("pcod")
    with pytest.raises(KeyError):
        assert df.loc["524 1 02"].value
    assert df.loc["524 4 12"].value == 29
