# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest

from flowapi.check_claims import check_claims


def test_invalid_claim_type():
    """
    Test only valid claims are allowed.
    """
    with pytest.raises(ValueError):
        check_claims("NOT_A_VALID_CLAIM")
