# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features import ModalLocation, ContactBalance, daily_location
from flowmachine.utils import list_of_dates
from flowmachine.features.subscriber.contact_modal_location_distance import *

import pytest


@pytest.mark.parametrize("statistic,msisdn,level,want", [
    ("avg", "gwAynWXp4eWvxGP7", "versioned-cell", 298.7215),
    ("stddev", "NG1km5NzBg5JD8nj", "versioned-site", 298.7215)
])
def test_contact_modal_location_distance(get_dataframe, statistic, msisdn, level, want):
    """ Test a few hand-picked ContactModalLocationDistance. """
    hl = ModalLocation(
        *[
            daily_location(d, level=level, method="last")
            for d in list_of_dates("2016-01-01", "2016-01-03")
        ]
    )
    cb = ContactBalance("2016-01-01", "2016-01-03")
    query = ContactModalLocationDistance(hl, cb, statistic="avg")
    df = get_dataframe(query).set_index("subscriber")
    print(df.sample(n=5))
    print()
    print(df.loc[msisdn])
    # assert df.loc[msisdn, f"distance_{statistic}"] == pytest.approx(want)

