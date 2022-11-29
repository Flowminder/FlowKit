# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Provides several convenient fixtures for working with JWTs in pytest
#

import os
from datetime import timedelta
from typing import Dict, Union, List, Callable, Optional

import pytest
from cryptography.hazmat.backends.openssl.rsa import _RSAPrivateKey, _RSAPublicKey
from cryptography.hazmat.primitives import serialization

from flowkit_jwt_generator import (
    load_private_key,
    load_public_key,
    get_all_claims_from_flowapi,
    generate_token,
)


@pytest.fixture
def private_key() -> _RSAPrivateKey:
    """
    Fixture which returns a private key loaded from a PRIVATE_JWT_SIGNING_KEY env var.
    """
    return load_private_key(os.environ["PRIVATE_JWT_SIGNING_KEY"])


@pytest.fixture
def public_key() -> _RSAPublicKey:
    """
    Fixture which returns a public key loaded from a PUBLIC_JWT_SIGNING_KEY env var.
    """
    return load_public_key(os.environ["PUBLIC_JWT_SIGNING_KEY"])


@pytest.fixture
def public_key_bytes(public_key) -> bytes:
    """
    Fixture which returns the PEM encoded bytestring representation of a  public key loaded
    from a PUBLIC_JWT_SIGNING_KEY env var.
    """
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


@pytest.fixture
def private_key_bytes(private_key) -> bytes:
    """
    Fixture which returns the PEM encoded bytestring representation of a private key loaded
    from a PRIVATE_JWT_SIGNING_KEY env var.
    """
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )


@pytest.fixture
def access_token_builder(audience: Optional[str] = None) -> Callable:
    """
    Fixture which builds short-life access tokens.

    Parameters
    ----------
    audience : str, default None
        Optionally specify an audience for the token, e.g. "TEST_SERVER".
        If not given will use the value of the FLOWAPI_IDENTIFIER env var if
        available, or None otherwise.


    Returns
    -------
    function
        Function which returns a token encoding the specified claims.

    """

    if audience is None and "FLOWAPI_IDENTIFIER" in os.environ:
        audience = os.environ["FLOWAPI_IDENTIFIER"]

    try:
        private_key = load_private_key(os.environ["PRIVATE_JWT_SIGNING_KEY"])
    except KeyError:
        raise EnvironmentError(
            "PRIVATE_JWT_SIGNING_KEY environment variable must be set."
        )

    def token_maker(roles: Dict) -> str:
        return generate_token(
            flowapi_identifier=audience,
            username="test",
            private_key=private_key,
            lifetime=timedelta(seconds=300),
            roles=roles,
        )

    return token_maker


@pytest.fixture
def universal_access_token(flowapi_url: str, access_token_builder: Callable) -> str:
    """

    Parameters
    ----------
    flowapi_url : str
    access_token_builder : pytest.fixture

    Returns
    -------
    str
        The token

    """
    return access_token_builder(
        {"universal_role": get_all_claims_from_flowapi(flowapi_url=flowapi_url)}
    )
