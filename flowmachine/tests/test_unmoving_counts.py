# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import make_spatial_unit
from flowmachine.features import UniqueLocations, SubscriberLocations
from flowmachine.features.subscriber.unmoving import Unmoving
from flowmachine.features.location.unmoving_counts import UnmovingCounts


def test_unmoving_counts_column_names(get_column_names_from_run):
    assert (
        get_column_names_from_run(
            UnmovingCounts(
                Unmoving(
                    locations=UniqueLocations(
                        SubscriberLocations("2016-01-01", "2016-01-01 10:00",
                                            spatial_unit=make_spatial_unit("admin", level=3))
                    )
                )
            )
        )
        == ["pcod", "value"]
    )


def test_unmoving_counts_values(get_dataframe):
    df = get_dataframe(
        UnmovingCounts(
            Unmoving(
                locations=UniqueLocations(
                    SubscriberLocations("2016-01-01", "2016-01-01 10:00",
                                        spatial_unit=make_spatial_unit("admin", level=3))
                )
            )
        )
    ).set_index("pcod")
    assert df.loc["524 1 02 09"].value == 5
    assert df.loc["524 2 04 20"].value == 7
