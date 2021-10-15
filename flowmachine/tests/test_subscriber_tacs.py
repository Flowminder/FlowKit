# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests the subscriber TAC information tools.
"""

import pytest

from flowmachine.features.subscriber import (
    SubscriberTACs,
    SubscriberTAC,
    SubscriberHandset,
    SubscriberHandsets,
    SubscriberHandsetCharacteristic,
)


@pytest.mark.parametrize(
    "query", [SubscriberTACs, SubscriberTAC, SubscriberHandset, SubscriberHandsets]
)
def test_column_names(query, get_dataframe):
    """Test that column_names attribute matches columns from calling head"""
    query_instance = query("2016-01-01", "2016-01-02")
    assert get_dataframe(query_instance).columns.tolist() == query_instance.column_names


def test_subscriber_tacs(get_dataframe):
    """
    Test that correct TACs are returned for one subscriber.
    """
    tc = get_dataframe(SubscriberTACs("2016-01-01", "2016-01-02")).set_index(
        "subscriber"
    )
    tacs = sorted(
        [42188591.0, 40909697.0, 48693702.0, 42188591.0, 92380772.0, 42188591.0]
    )
    assert sorted(tc.loc["1p4MYbA1Y4bZzBQa"].tac.tolist()) == tacs


def test_modal_tac(get_dataframe):
    """
    Test that most common tacs are right.
    """

    tc = get_dataframe(SubscriberTACs("2016-01-01", "2016-01-02")).set_index(
        "subscriber"
    )
    assert (
        tc.loc["0DB8zw67E9mZAPK2"].tac.mode()[0]
        == get_dataframe(SubscriberTAC("2016-01-01", "2016-01-02"))
        .set_index("subscriber")
        .loc["0DB8zw67E9mZAPK2"]
        .tac
    )


def test_last_tac(get_dataframe):
    """Test that last used TAC is right."""
    tc = (
        get_dataframe(SubscriberTACs("2016-01-01", "2016-01-02"))
        .sort_values(by=["subscriber", "time"])
        .set_index("subscriber")
    )
    assert (
        tc.loc["zvaOknzKbEVD2eME"].tac.tolist()[-1]
        == get_dataframe(SubscriberTAC("2016-01-01", "2016-01-02", method="last"))
        .set_index("subscriber")
        .loc["zvaOknzKbEVD2eME"]
        .tac
    )


def test_tac_errors():
    """Test that correct ValueErrors are raised."""
    with pytest.raises(ValueError, match="foo is not a valid method"):
        SubscriberTAC("2016-01-01", "2016-01-02", method="foo")


def test_imei_warning():
    """Test that a warning is issued when imei is used as identifier."""
    with pytest.warns(UserWarning):
        SubscriberTAC("2016-01-01", "2016-01-02", subscriber_identifier="imei")


def test_subscriber_handsets(get_dataframe):
    """
    Test that correct handsets are returned for one subscriber.
    """
    tc = get_dataframe(SubscriberHandsets("2016-01-01", "2016-01-02")).set_index(
        "subscriber"
    )
    tacs = sorted(["LB-01", "GK-00", "VY-01", "LB-01", "LM-34", "LB-01"])
    assert sorted(tc.loc["1p4MYbA1Y4bZzBQa"].model.tolist()) == tacs


def test_subscriber_handset(get_dataframe):
    """
    Test that correct handset is returned for one subscriber.
    """
    tc = get_dataframe(SubscriberHandset("2016-01-01", "2016-01-02")).set_index(
        "subscriber"
    )
    assert tc.loc["1p4MYbA1Y4bZzBQa"].model == "LB-01"


def test_subscriber_handset_characteristic(get_dataframe):
    """Check that correct handset characteristic is returned for selected subscribers."""

    assert (
        get_dataframe(
            SubscriberHandsetCharacteristic("2016-01-01", "2016-01-07", "hnd_type")
        )
        .set_index("subscriber")
        .loc["038OVABN11Ak4W5P"]
        .value
        == "Smart"
    )

    assert (
        get_dataframe(
            SubscriberHandsetCharacteristic(
                "2016-01-01", "2016-01-07", "brand", method="last"
            )
        )
        .set_index("subscriber")
        .loc["YMBqRkzbbxGkX3zA"]
        .value
        == "Sony"
    )


def test_subscriber_handset_characteristic_errors():
    """Check that ValueErrors are correctly raised."""
    with pytest.raises(ValueError, match="foo is not a valid characteristic"):
        SubscriberHandsetCharacteristic(
            "2016-01-01", "2016-01-07", "foo", method="last"
        )

    with pytest.raises(ValueError, match="foo is not a valid method"):
        SubscriberHandsetCharacteristic(
            "2016-01-01", "2016-01-07", "hnd_type", method="foo"
        )
