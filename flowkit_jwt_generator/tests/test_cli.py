# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
import jwt
from click.testing import CliRunner

from flowkit_jwt_generator.jwt import print_token, print_all_access_token


def test_universal_token_builder(dummy_flowapi):
    """
    Test the universal access token builder cli works as expected.
    """
    runner = CliRunner()
    result = runner.invoke(
        print_token,
        ["DUMMY_USER", os.environ["JWT_SECRET_KEY"], 1, "--all-access", "DUMMY_URL"],
    )
    decoded = jwt.decode(
        jwt=result.output.strip(),
        key=os.environ["JWT_SECRET_KEY"],
        verify=True,
        algorithms=["HS256"],
    )
    assert result.exit_code == 0
    assert decoded["user_claims"] == dummy_flowapi


def test_token_builder():
    """
    Test the arbitrary token builder cli works as expected.
    """
    runner = CliRunner()
    result = runner.invoke(
        print_token,
        [
            "DUMMY_USER",
            os.environ["JWT_SECRET_KEY"],
            1,
            "--query",
            "-p",
            "run",
            "-p",
            "poll",
            "DUMMY_QUERY",
            "--query",
            "-a",
            "admin3",
            "-a",
            "admin0",
            "DUMMY_QUERY_B",
        ],
    )
    decoded = jwt.decode(
        jwt=result.output.strip(),
        key=os.environ["JWT_SECRET_KEY"],
        verify=True,
        algorithms=["HS256"],
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


def test_token_builder_prompts_with_no_perms():
    """
    Test the arbitrary token builder cli prompts for confirmation if a claimset will be empty.
    """
    runner = CliRunner()
    result = runner.invoke(
        print_token,
        ["DUMMY_USER", os.environ["JWT_SECRET_KEY"], 1, "--query", "DUMMY_QUERY"],
    )
    assert result.exit_code == 1
