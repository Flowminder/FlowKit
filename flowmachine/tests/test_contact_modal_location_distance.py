# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.contact_modal_location_distance import *

import pytest


@pytest.mark.parametrize(
    "statistic,msisdn,want",
    [
        ("avg", "gwAynWXp4eWvxGP7", 298.7215),
        ("stddev", "V7MBRewnwQGE91gY", 182.519128),
    ],
)
def test_contact_modal_location_distance(get_dataframe, statistic, msisdn, want):
    """ Test a few hand-picked ContactModalLocationDistance. """
    query = ContactModalLocationDistance("2016-01-01", "2016-01-03", statistic=statistic)
    print(query.get_query())
    df = get_dataframe(query).set_index("subscriber")
    print(df.sample(n=5))
    assert df.loc[msisdn, f"distance_{statistic}"] == pytest.approx(want)

