# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the VersionedInfrastructure() class.
"""

import pytest

from flowmachine.features import VersionedInfrastructure


def test_returns_correct_results(get_length):
    """
    VersionedInfrastructure() returns N-sized result set.
    """
    result = VersionedInfrastructure()
    assert 30 == len(result)


def test_raises_error_if_wrong_table_used():
    """
    VersionedInfrastructure() raises error if not using 'sites' or 'cells' tables.
    """
    with pytest.raises(ValueError):
        VersionedInfrastructure(table="xxx")
