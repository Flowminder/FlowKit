# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.contact_reciprocal import *
import pandas as pd

import pytest


def test_contact_reciprocal(get_dataframe):
    """ Test a few cases of ContactReciprocal. """
    query = ContactReciprocal("2016-01-01", "2016-01-08", exclude_self_calls=False)
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].reciprocal
    assert not df.loc["9vXy462Ej8V1kpWl"].reciprocal.values[0]


def test_proportion_contact_reciprocal(get_dataframe):
    """ Test a few cases of ProportionContactReciprocal. """
    query = ProportionContactReciprocal(
        "2016-01-01", "2016-01-08", exclude_self_calls=False
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].proportion == 1
    assert df.loc["9vXy462Ej8V1kpWl"].proportion == 0


def test_proportion_reciprocal(get_dataframe):
    """ Test a few cases of ProportionEventReciprocal. """
    query = ProportionEventReciprocal(
        "2016-01-01", "2016-01-08", exclude_self_calls=False
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].value == 1
    assert df.loc["9vXy462Ej8V1kpWl"].value == 0

    query = ProportionEventReciprocal(
        "2016-01-01",
        "2016-01-08",
        subscriber_identifier="imei",
        exclude_self_calls=False,
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[0] == pytest.approx(0.00009999)

    contact_reciprocal = ContactReciprocal(
        "2016-01-02", "2016-01-03", exclude_self_calls=False
    )
    query = ProportionEventReciprocal(
        "2016-01-01",
        "2016-01-08",
        contact_reciprocal=contact_reciprocal,
        exclude_self_calls=False,
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].value == 0
    assert df.loc["9vXy462Ej8V1kpWl"].value == 0

    query = ProportionEventReciprocal(
        "2016-01-01", "2016-01-08", direction="in", exclude_self_calls=False
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].value == 1
    assert df.loc["9vXy462Ej8V1kpWl"].value == 0
