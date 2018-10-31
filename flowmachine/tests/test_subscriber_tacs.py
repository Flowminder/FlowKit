# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests the subscriber TAC information tools.
"""

from unittest import TestCase
import pytest

from flowmachine.features.subscriber import (
    SubscriberTACs,
    SubscriberTAC,
    SubscriberHandset,
    SubscriberHandsets,
    SubscriberPhoneType,
)


@pytest.mark.parametrize(
    "query",
    [
        SubscriberTACs,
        SubscriberTAC,
        SubscriberHandset,
        SubscriberHandsets,
        SubscriberPhoneType,
    ],
)
def test_column_names(query):
    """Test that column_names attribute matches columns from calling head"""
    query_instance = query("2016-01-01", "2016-01-02")
    assert query_instance.head(0).columns.tolist() == query_instance.column_names


class TestTACs(TestCase):
    def test_subscriber_tacs(self):
        """
        Test that correct TACs are returned for one subscriber.
        """
        tc = (
            SubscriberTACs("2016-01-01", "2016-01-02")
            .get_dataframe()
            .set_index("subscriber")
        )
        tacs = sorted(
            [42188591.0, 40909697.0, 48693702.0, 42188591.0, 92380772.0, 42188591.0]
        )
        self.assertListEqual(sorted(tc.ix["1p4MYbA1Y4bZzBQa"].tac.tolist()), tacs)

    def test_modal_tac(self):
        """
        Test that most common tacs are right.
        """

        tc = (
            SubscriberTACs("2016-01-01", "2016-01-02")
            .get_dataframe()
            .set_index("subscriber")
        )
        self.assertEqual(
            tc.ix["0DB8zw67E9mZAPK2"].tac.mode()[0],
            SubscriberTAC("2016-01-01", "2016-01-02")
            .get_dataframe()
            .set_index("subscriber")
            .ix["0DB8zw67E9mZAPK2"]
            .tac,
        )

    def test_last_tac(self):
        """Test that last used TAC is right."""
        tc = (
            SubscriberTACs("2016-01-01", "2016-01-02")
            .get_dataframe()
            .sort_values(by=["subscriber", "time"])
            .set_index("subscriber")
        )
        self.assertEqual(
            tc.ix["zvaOknzKbEVD2eME"].tac.tolist()[-1],
            SubscriberTAC("2016-01-01", "2016-01-02", method="last")
            .get_dataframe()
            .set_index("subscriber")
            .ix["zvaOknzKbEVD2eME"]
            .tac,
        )

    def test_imei_warning(self):
        """Test that a warning is issued when imei is used as identifier."""
        with self.assertWarns(UserWarning):
            SubscriberTAC("2016-01-01", "2016-01-02", subscriber_identifier="imei")


class TestHandsets(TestCase):
    def test_subscriber_handsets(self):
        """
        Test that correct handsets are returned for one subscriber.
        """
        tc = (
            SubscriberHandsets("2016-01-01", "2016-01-02")
            .get_dataframe()
            .set_index("subscriber")
        )
        tacs = sorted(["LB-01", "GK-00", "VY-01", "LB-01", "LM-34", "LB-01"])
        self.assertListEqual(sorted(tc.ix["1p4MYbA1Y4bZzBQa"].model.tolist()), tacs)

    def test_subscriber_handset(self):
        """
        Test that correct handset is returned for one subscriber.
        """
        tc = (
            SubscriberHandset("2016-01-01", "2016-01-02")
            .get_dataframe()
            .set_index("subscriber")
        )
        self.assertEqual(tc.ix["1p4MYbA1Y4bZzBQa"].model, "LB-01")


class TestPhoneTypes(TestCase):
    def test_subscriber_phonetype(self):
        """Check that correct smart/feature label is returned."""
        self.assertEqual(
            SubscriberPhoneType("2016-01-01", "2016-01-07")
            .get_dataframe()
            .set_index("subscriber")
            .ix["038OVABN11Ak4W5P"]
            .handset_type,
            "Smart",
        )
        self.assertEqual(
            SubscriberPhoneType("2016-01-01", "2016-01-07", method="last")
            .get_dataframe()
            .set_index("subscriber")
            .ix["YMBqRkzbbxGkX3zA"]
            .handset_type,
            "Feature",
        )
