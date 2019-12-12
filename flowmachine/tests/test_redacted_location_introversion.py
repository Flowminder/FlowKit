# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.features import LocationIntroversion
from flowmachine.features.location.redacted_location_introversion import (
    RedactedLocationIntroversion,
)


def test_unsafe_values_hidden(get_dataframe):
    """
    Test that redacted location introversion does not return results when they're premised on less than
    16 people.
    """
    rli = RedactedLocationIntroversion(
        location_introversion=LocationIntroversion(
            "2016-01-01", "2016-01-02", hours=(17, 18)
        )
    )
    assert len(get_dataframe(rli)) == 0
