# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.contact_modal_location_distance import *

import pytest
from flowmachine.features.subscriber.contact_balance import ContactBalance


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
    assert df.value[msisdn] == pytest.approx(want)

@pytest.mark.parametrize("kwarg", ["statistic", "contact_balance"])
def test_contact_modal_location_errors(kwarg):
    """ Test ValueError is raised for non-compliant kwarg in ContactModalLocationDistance. """

    error = (
        "error" if kwarg not in {"contact_balance"} 
        else ContactBalance("2016-01-03", "2016-01-05", subscriber_identifier="imei")
    )

    with pytest.raises(ValueError):
        query = ContactModalLocationDistance("2016-01-03", "2016-01-05", **{kwarg: error})
