# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def test_plugin_errors_with_missing_env_var(testdir, monkeypatch):
    testdir.makepyfile(
        """
        def test_missing_env_var(access_token_builder):
            assert True
    """
    )
    monkeypatch.delenv("JWT_SECRET_KEY")
    result = testdir.runpytest()

    result.assert_outcomes(error=1)


def test_plugin_works_with_flowapi_fixture(testdir, dummy_flowapi):
    """Universal access token plugin should pick up url from flowapi fixture."""

    # create a temporary conftest.py file
    testdir.makeconftest(
        """
        import pytest

        @pytest.fixture
        def flowapi_url():
            return "http://flowapi"
    """
    )

    # create a temporary pytest test file
    testdir.makepyfile(
        f"""
        import jwt
        import os
        
        def test_all_access_plugin(universal_access_token):
            decoded = jwt.decode(
                jwt=universal_access_token,
                audience=os.environ["FLOWAPI_IDENTIFIER"],
                key=os.environ["JWT_SECRET_KEY"],
                verify=True,
                algorithms=["HS256"],
            )
            assert decoded["user_claims"] == {dummy_flowapi}
    """
    )

    # run all tests with pytest
    result = testdir.runpytest()

    result.assert_outcomes(passed=1)
