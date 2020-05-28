# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import base64
from datetime import timedelta

import jwt

import pytest
from flowkit_jwt_generator.jwt import generate_token, squashed_scopes, decompress_claims


@pytest.mark.parametrize(
    "scopes, expected",
    [
        (["SCOPE"], ["SCOPE"]),
        (["get_result&SCOPE", "run&SCOPE"], ["get_result,run&SCOPE"]),
        (["get_result&SCOPE.foo", "run&SCOPE"], ["get_result&SCOPE.foo", "run&SCOPE"]),
        (
            [
                "get_result&SCOPE.aggregation_unit.foo",
                "get_result&SCOPE.aggregation_unit.bar",
                "run&SCOPE.aggregation_unit.bar",
                "run&SCOPE.aggregation_unit.foo",
            ],
            ["get_result,run&SCOPE.aggregation_unit.bar,SCOPE.aggregation_unit.foo"],
        ),
        (
            [
                "run&SCOPE.aggregation_unit.bar",
                "run&SCOPE.aggregation_unit.foo",
                "run&SCOPE.aggregation_unit.baz",
            ],
            [
                "run&SCOPE.aggregation_unit.bar,SCOPE.aggregation_unit.baz,SCOPE.aggregation_unit.foo"
            ],
        ),
    ],
)
def test_scope_squashing(scopes, expected):
    assert list(squashed_scopes(scopes)) == expected


def test_token_generator(private_key, public_key):
    """Test that the baseline token generator behaves as expected"""
    token = generate_token(
        username="test",
        private_key=private_key,
        lifetime=timedelta(seconds=90),
        claims=["A_CLAIM"],
    )
    decoded = jwt.decode(jwt=token, key=public_key, verify=True, algorithms=["RS256"])
    assert decoded["identity"] == "test"
    assert decompress_claims(decoded["user_claims"]) == ["A_CLAIM"]
    assert "aud" not in decoded


def test_token_generator_with_audience(private_key, public_key):
    """Test that the baseline token generator behaves as expected when given an audience"""
    token = generate_token(
        flowapi_identifier="test_audience",
        username="test",
        private_key=private_key,
        lifetime=timedelta(seconds=90),
        claims=["A_CLAIM"],
    )
    decoded = jwt.decode(
        jwt=token,
        key=public_key,
        verify=True,
        algorithms=["RS256"],
        audience="test_audience",
    )
    assert decoded["identity"] == "test"
    assert decompress_claims(decoded["user_claims"]) == ["A_CLAIM"]
