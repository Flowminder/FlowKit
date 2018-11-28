# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the VersionedInfrastructure() class.
"""

import pytest

from flowmachine.features import VersionedInfrastructure


@pytest.mark.parametrize("table, expected_count", [("sites", 30), ("cells", 55)])
def test_returns_correct_results(table, expected_count, get_length):
    """
    VersionedInfrastructure() returns N-sized result set.
    """
    result = VersionedInfrastructure(table=table)
    assert expected_count == get_length(result)


def test_returns_in_date_results(get_length):
    """
    VersionedInfrastructure doesn't include cells not active on a date.
    """
    result = VersionedInfrastructure(table="cells", date="2016-01-05")
    assert 48 == get_length(result)


def test_raises_error_if_wrong_table_used():
    """
    VersionedInfrastructure() raises error if not using 'sites' or 'cells' tables.
    """
    with pytest.raises(ValueError):
        VersionedInfrastructure(table="xxx")
