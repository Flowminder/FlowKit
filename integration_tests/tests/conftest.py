# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import timedelta

import pytest
import os

import requests
from flask_jwt_extended.tokens import encode_access_token
from quart.json import JSONEncoder
from .utils import make_token


@pytest.fixture
def api_host():
    """
    Fixture for getting the url of the available API server.
    Primarily to allow the tests to run locally, or in a docker container.

    Returns
    -------
    str
        URL of running API container
    """
    in_docker_host = "http://flowapi:9090"
    try:
        requests.get(in_docker_host, verify=False)
        return in_docker_host
    except requests.ConnectionError:
        return "http://localhost:9090"


@pytest.fixture
def access_token_builder():
    """
    Fixture which builds short-life access tokens.

    Returns
    -------
    function
        Function which returns a token encoding the specified claims.
    """
    secret = os.getenv("JWT_SECRET_KEY")
    if secret is None:
        raise EnvironmentError("JWT_SECRET_KEY environment variable not set.")

    def token_maker(claims):
        return make_token("test", secret, timedelta(seconds=90), claims)

    return token_maker
