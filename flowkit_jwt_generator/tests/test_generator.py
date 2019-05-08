# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import timedelta

import jwt

from flowkit_jwt_generator.jwt import generate_token


def test_token_generator():
    """Test that the baseline token generator behaves as expected"""
    token = generate_token(
        username="test",
        secret="secret",
        lifetime=timedelta(seconds=90),
        claims={"A_CLAIM": "A_VALUE"},
    )
    decoded = jwt.decode(jwt=token, key="secret", verify=True, algorithms=["HS256"])
    assert decoded["identity"] == "test"
    assert decoded["user_claims"] == {"A_CLAIM": "A_VALUE"}
