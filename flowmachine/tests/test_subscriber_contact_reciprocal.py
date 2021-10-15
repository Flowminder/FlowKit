# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.contact_reciprocal import *
import pandas as pd

import pytest


def test_contact_reciprocal(get_dataframe):
    """Test a few cases of ContactReciprocal."""
    query = ContactReciprocal("2016-01-01", "2016-01-08", exclude_self_calls=False)
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].reciprocal
    assert not df.loc["9vXy462Ej8V1kpWl"].reciprocal.values[0]


def test_proportion_contact_reciprocal(get_dataframe):
    """Test a few cases of ProportionContactReciprocal."""
    query = ProportionContactReciprocal(
        ContactReciprocal("2016-01-01", "2016-01-08", exclude_self_calls=False)
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].proportion == 1
    assert df.loc["9vXy462Ej8V1kpWl"].proportion == 0


def test_proportion_reciprocal(get_dataframe):
    """Test a few cases of ProportionEventReciprocal."""
    query = ProportionEventReciprocal(
        "2016-01-01",
        "2016-01-08",
        ContactReciprocal("2016-01-01", "2016-01-08", exclude_self_calls=False),
        exclude_self_calls=False,
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].value == 1
    assert df.loc["9vXy462Ej8V1kpWl"].value == 0

    query = ProportionEventReciprocal(
        "2016-01-01",
        "2016-01-08",
        ContactReciprocal("2016-01-01", "2016-01-08", exclude_self_calls=False),
        subscriber_identifier="imei",
        exclude_self_calls=False,
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[0] == pytest.approx(0.00009997)

    query = ProportionEventReciprocal(
        "2016-01-01",
        "2016-01-08",
        ContactReciprocal("2016-01-01", "2016-01-08", exclude_self_calls=False),
        direction="in",
        exclude_self_calls=False,
    )
    df = get_dataframe(query).set_index("subscriber")
    assert df.loc["self_caller"].value == 1
    assert df.loc["9vXy462Ej8V1kpWl"].value == 0

    query = ProportionEventReciprocal(
        "2016-01-01",
        "2016-01-08",
        ContactReciprocal("2016-01-01", "2016-01-08", exclude_self_calls=False),
        direction="in",
        exclude_self_calls=True,
    )
    df = get_dataframe(query).set_index("subscriber")
    assert "self_caller" not in df.index
    assert df.value.unique() == [0]


@pytest.mark.parametrize("kwarg", ["direction"])
def test_proportion_event_reciprocal_errors(kwarg):
    """Test ValueError is raised for non-compliant kwarg in ProportionEventReciprocal."""

    with pytest.raises(ValueError):
        query = ProportionEventReciprocal(
            "2016-01-03",
            "2016-01-05",
            ContactReciprocal("2016-01-03", "2016-01-05"),
            **{kwarg: "error"}
        )
