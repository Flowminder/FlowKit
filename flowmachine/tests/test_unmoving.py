# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import make_spatial_unit
from flowmachine.features import UniqueLocations, SubscriberLocations
from flowmachine.features.subscriber.unmoving import Unmoving


def test_unmoving_column_names(get_column_names_from_run):
    assert get_column_names_from_run(
        Unmoving(
            locations=UniqueLocations(
                SubscriberLocations(
                    "2016-01-01",
                    "2016-01-01 10:00",
                    spatial_unit=make_spatial_unit("admin", level=3),
                )
            )
        )
    ) == ["subscriber", "value"]


def test_unmoving_values(get_dataframe):
    df = get_dataframe(
        Unmoving(
            locations=UniqueLocations(
                SubscriberLocations(
                    "2016-01-01",
                    "2016-01-01 10:00",
                    spatial_unit=make_spatial_unit("admin", level=3),
                )
            )
        )
    ).set_index("subscriber")
    assert not df.loc["038OVABN11Ak4W5P"].value
    assert df.loc["0Gl95NRLjW2aw8pW"].value
