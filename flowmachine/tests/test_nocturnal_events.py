# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pytest

from flowmachine.features import NocturnalEvents


def test_has_right_columns():
    """
    NocturnalEvents() dataframe returns the right columns.
    """
    noc = NocturnalEvents("2016-01-01", "2016-01-02")
    expected_columns = ["subscriber", "value"]
    assert noc.column_names == expected_columns


def test_nocturnal_events(get_dataframe):
    """
    NocturnalEvents() returns correct values for a few hand picked values.
    """
    noc = NocturnalEvents("2016-01-01", "2016-01-02")
    df = get_dataframe(noc)
    df.set_index("subscriber", inplace=True)
    assert df.value["BKMy1nYEZpnoEA7G"] == pytest.approx(57.142857)
    assert df.value["dM4aLP8N97eYABwR"] == pytest.approx(33.333333)

    noc = NocturnalEvents("2016-01-01", "2016-01-05", direction="in")
    df = get_dataframe(noc)
    df.set_index("subscriber", inplace=True)
    assert df.value["BKMy1nYEZpnoEA7G"] == pytest.approx(50.0)
    assert df.value["dM4aLP8N97eYABwR"] == pytest.approx(25.0)


@pytest.mark.parametrize("kwarg", ["direction"])
def test_nocturnal_errors(kwarg):
    """Test ValueError is raised for non-compliant kwarg in NocturnalEvents."""

    with pytest.raises(ValueError):
        query = NocturnalEvents("2016-01-03", "2016-01-05", **{kwarg: "error"})
