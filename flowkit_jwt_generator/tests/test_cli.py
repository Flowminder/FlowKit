# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import jwt
from click.testing import CliRunner

from flowkit_jwt_generator.jwt import print_token, print_all_access_token


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
            "--audience",
            os.environ["FLOWAPI_IDENTIFIER"],
            "all-access",
            "--flowapi-url",
            "DUMMY_URL",
        ],
    )
    decoded = jwt.decode(
        jwt=result.output.strip(),
        key=public_key,
        verify=True,
        algorithms=["RS256"],
        audience=os.environ["FLOWAPI_IDENTIFIER"],
    )
    assert result.exit_code == 0
    assert decoded["user_claims"] == dummy_flowapi


def test_token_builder(private_key_bytes, public_key):
    """
    Test the arbitrary token builder cli works as expected.
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
            "--audience",
            os.environ["FLOWAPI_IDENTIFIER"],
            "query",
            "-p",
            "run",
            "-p",
            "poll",
            "--query-name",
            "DUMMY_QUERY",
            "query",
            "-a",
            "admin3",
            "-a",
            "admin0",
            "--query-name",
            "DUMMY_QUERY_B",
        ],
    )
    decoded = jwt.decode(
        jwt=result.output.strip(),
        key=public_key,
        verify=True,
        algorithms=["RS256"],
        audience=os.environ["FLOWAPI_IDENTIFIER"],
    )
    assert result.exit_code == 0
    assert decoded["identity"] == "DUMMY_USER"
    assert decoded["user_claims"]["DUMMY_QUERY"] == {
        "permissions": {"run": True, "poll": True},
        "spatial_aggregation": [],
    }
    assert decoded["user_claims"]["DUMMY_QUERY_B"] == {
        "permissions": {},
        "spatial_aggregation": ["admin3", "admin0"],
    }


def test_token_builder_prompts_with_no_perms(private_key_bytes):
    """
    Test the arbitrary token builder cli prompts for confirmation if a claimset will be empty.
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
            "--audience",
            os.environ["FLOWAPI_IDENTIFIER"],
            "query",
            "--query-name",
            "DUMMY_QUERY",
        ],
    )
    assert result.exit_code == 1
