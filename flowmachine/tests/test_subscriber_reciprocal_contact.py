# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.reciprocal_contact import *
from flowmachine.core.errors.flowmachine_errors import MissingColumnsError
import pandas as pd

import pytest


def test_reciprocal_contact(get_dataframe):
    """ Test a few cases of ReciprocalContact. """
    query = ReciprocalContact("2016-01-01", "2016-01-08", exclude_self_calls=False)
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].reciprocal

    query = ReciprocalContact("2016-01-01", "2016-01-08", exclude_self_calls=False)
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].reciprocal


def test_proportion_reciprocal_contact(get_dataframe):
    """ Test a few cases of ProportionReciprocal. """
    query = ProportionReciprocal("2016-01-01", "2016-01-08", exclude_self_calls=False)
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].proportion == 1

    query = ProportionReciprocal(
        "2016-01-01",
        "2016-01-08",
        subscriber_identifier="imei",
        exclude_self_calls=False,
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.proportion[0] == pytest.approx(0.00009999)

    reciprocal_contact = ReciprocalContact(
        "2016-01-02", "2016-01-03", exclude_self_calls=False
    )
    query = ProportionReciprocal(
        "2016-01-01",
        "2016-01-08",
        reciprocal_contact=reciprocal_contact,
        exclude_self_calls=False,
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].proportion == 0

    query = ProportionReciprocal(
        "2016-01-01", "2016-01-08", direction="in", exclude_self_calls=False
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].proportion == 1
