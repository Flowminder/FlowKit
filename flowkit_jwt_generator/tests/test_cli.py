# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import jwt
from click.testing import CliRunner

from flowkit_jwt_generator.jwt import decompress_claims
from flowkit_jwt_generator.cli import print_token


def test_universal_token_builder(dummy_flowapi, public_key, private_key_bytes):
    """
    Test the universal access token builder cli works as expected.
    """
    runner = CliRunner()
    result = runner.invoke(
        print_token,
        [
            "--username",
            "DUMMY_USER",
            "--private-key",
            private_key_bytes,
            "--lifetime",
            1,
            "--flowapi-url",
            "DUMMY_URL",
        ],
    )
    decoded = jwt.decode(
        jwt=result.output.strip(),
        key=public_key,
        algorithms=["RS256"],
        audience=dummy_flowapi["aud"],
    )
    assert result.exit_code == 0
    assert decompress_claims(decoded["user_claims"]) == dummy_flowapi["claims"]
    assert decoded["aud"] == dummy_flowapi["aud"]
