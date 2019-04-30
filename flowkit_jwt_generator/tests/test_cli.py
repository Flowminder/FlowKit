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
        print_all_access_token,
        ["--username", "DUMMY_USER", "--secret-key", os.environ["JWT_SECRET_KEY"]],
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
            "--username",
            "DUMMY_USER",
            "--secret-key",
            os.environ["JWT_SECRET_KEY"],
            "-p",
            "DUMMY_QUERY",
            "run",
            "-p" "DUMMY_QUERY",
            "poll",
            "-a",
            "DUMMY_QUERY_B",
            "DUMMY_AGGREGATION",
            "-a",
            "DUMMY_QUERY_B",
            "DUMMY_AGGREGATION_B",
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
        "spatial_aggregation": ["DUMMY_AGGREGATION", "DUMMY_AGGREGATION_B"],
    }
