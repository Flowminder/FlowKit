# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the feature subscriber event proportions.
"""

import pytest

from flowmachine.features.subscriber.proportion_outgoing import ProportionOutgoing


"""
Tests for ProportionOutgoing() feature class.
"""


def test_returns_correct_column_names():
    """
    ProportionOutgoing() dataframe has expected column names.
    """
    ud = ProportionOutgoing("2016-01-01", "2016-01-04")
    assert [
        "subscriber",
        "proportion_outgoing",
        "proportion_incoming",
    ] == ud.column_names


def test_returns_correct_values(get_dataframe):
    """
    ProportionOutgoing() dataframe contains expected values.
    """
    ud = ProportionOutgoing("2016-01-01", "2016-01-04")
    df1 = get_dataframe(ud).set_index("subscriber")
    assert 0.600000 == df1.loc["ZM3zYAPqx95Rw15J"]["proportion_outgoing"]
    assert 0.400000 == df1.loc["ZM3zYAPqx95Rw15J"]["proportion_incoming"]


def test_passing_not_known_subscriber_identifier_raises_error():
    """
    ProportionOutgoing() passing not know `subscriber_identifier` raises error.
    """
    with pytest.raises(ValueError):
        ProportionOutgoing("2016-01-01", "2016-01-04", subscriber_identifier="foo")
