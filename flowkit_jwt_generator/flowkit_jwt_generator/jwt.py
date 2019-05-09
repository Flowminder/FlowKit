# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import base64
import datetime
import os
import uuid
from binascii import Error
from json import JSONEncoder
from typing import Dict, List, Union, Callable, Tuple, Optional
from collections import ChainMap
from datetime import timedelta
import click
import jwt
import pytest
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.backends.openssl.rsa import _RSAPrivateKey, _RSAPublicKey
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

permissions_types = {"run": True, "poll": True, "get_result": True}
aggregation_types = ["admin0", "admin1", "admin2", "admin3", "admin4"]


def load_private_key(key_string: str) -> _RSAPrivateKey:
    """
    Load a private key from a string, which may be base64 encoded.

    Parameters
    ----------
    key_string : str
        String containing the key, optionally base64 encoded

    Returns
    -------
    _RSAPrivateKey
        The private key
    """
    try:
        return serialization.load_pem_private_key(
            key_string.encode(), password=None, backend=default_backend()
        )
    except ValueError:
        try:
            return load_private_key(base64.b64decode(key_string).decode())
        except (Error, ValueError):
            raise ValueError("Failed to load key.")


def load_public_key(key_string: str) -> _RSAPublicKey:
    """
    Load a public key from a string, which may be base64 encoded.

    Parameters
    ----------
    key_string : str
        String containing the key, optionally base64 encoded

    Returns
    -------
    _RSAPubliceKey
        The public key
    """
    try:
        return serialization.load_pem_public_key(
            key_string.encode(), backend=default_backend()
        )
    except ValueError:
        try:
            return load_public_key(base64.b64decode(key_string).decode())
        except (Error, ValueError):
            raise ValueError("Failed to load key.")


def generate_keypair() -> Tuple[bytes, bytes]:
    """
    Generate an RSA key pair.

    Returns
    -------
    tuple of bytes, bytes
        Private key, public key

    Notes
    -----
    You will typically want to base64 encode the keys if planning to use them as environment variables.
    """
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=4096, backend=default_backend()
    )
    public_key_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return private_key_bytes, public_key_bytes


def generate_token(
    *,
    audience: Optional[str] = None,
    username: str,
    secret: str,
    lifetime: datetime.timedelta,
    claims: Dict[str, Dict[str, Union[Dict[str, bool], List[str]]]],
) -> str:
    """

    Parameters
    ----------
    username : str
        Username for the token
    secret : str
        Shared secret to sign the token with
    lifetime : datetime.timedelta
        Lifetime from now of the token
    claims : dict
        Dictionary of claims the token will grant
    audience : str, optional
        Optionally provide a string to identify the audience of the token

    Examples
    --------
    >>> generate_token(username="TEST_USER", secret="SECRET", lifetime=datetime.timedelta(5), audience="TEST_SERVER", claims={"daily_location":{"permissions": {"run":True},
            "spatial_aggregation": ["admin3"]}})
    'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1NTc0MDM1OTgsIm5iZiI6MTU1NzQwMzU5OCwianRpIjoiZjIwZmRlYzYtYTA4ZS00Y2VlLWJiODktYjc4OGJhNjcyMDFiIiwidXNlcl9jbGFpbXMiOnsiZGFpbHlfbG9jYXRpb24iOnsicGVybWlzc2lvbnMiOnsicnVuIjp0cnVlfSwic3BhdGlhbF9hZ2dyZWdhdGlvbiI6WyJhZG1pbjMiXX19LCJpZGVudGl0eSI6IlRFU1RfVVNFUiIsImV4cCI6MTU1NzgzNTU5OCwiYXVkIjoiVEVTVF9TRVJWRVIifQ.yxBFYZ2EFyVKdVT9Sc-vC6qUpwRNQHt4KcOdFrQ4YrI'

    Returns
    -------
    str
        Encoded token

    """

    now = datetime.datetime.utcnow()
    token_data = dict(
        iat=now,
        nbf=now,
        jti=str(uuid.uuid4()),
        user_claims=claims,
        identity=username,
        exp=now + lifetime,
    )
    if audience is not None:
        token_data["aud"] = audience
    return jwt.encode(
        payload=token_data, key=secret, algorithm="HS256", json_encoder=JSONEncoder
    ).decode("utf-8")


@pytest.fixture
def access_token_builder(audience: Optional[str] = None) -> Callable:
    """
    Fixture which builds short-life access tokens.

    Parameters
    ----------
    audience : str, default None
        Optionally specify an audience for the token

    Returns
    -------
    function
        Function which returns a token encoding the specified claims.
    """

    if audience is None and "audience" in os.environ:
        audience = os.environ["FLOWAPI_IDENTIFIER"]

    secret = os.getenv("JWT_SECRET_KEY")
    if secret is None:
        raise EnvironmentError("JWT_SECRET_KEY environment variable not set.")

    def token_maker(
        claims: Dict[str, Dict[str, Union[Dict[str, bool], List[str]]]]
    ) -> str:
        return generate_token(
            username="test",
            audience=audience,
            secret=secret,
            lifetime=timedelta(seconds=90),
            claims=claims,
        )

    return token_maker


def get_all_claims_from_flowapi(
    flowapi_url: str
) -> Dict[str, Dict[str, Dict[str, Union[Dict[str, bool], List[str]]]]]:
    """
    Retrieve all the query types available on an instance of flowapi and
    generate a claims dictionary which grants total access to them.

    Parameters
    ----------
    flowapi_url : str
        URL of the flowapi instance

    Returns
    -------
    dict
        Claims dictionary

    """
    query_kinds = []
    aggregation_types = set()
    spec = requests.get(f"{flowapi_url}/api/0/spec/openapi.json").json()
    for q_class, q_spec in spec["components"]["schemas"].items():
        try:
            query_kinds.append(q_spec["properties"]["query_kind"]["enum"][0])
            aggregation_types.update(q_spec["properties"]["aggregation_unit"]["enum"])
        except KeyError:
            pass
    query_kinds = sorted(query_kinds)
    aggregation_types = sorted(aggregation_types)
    all_claims = {
        query_kind: {
            "permissions": permissions_types,
            "spatial_aggregation": aggregation_types,
        }
        for query_kind in query_kinds
    }
    all_claims.update(
        {
            "geography": {
                "permissions": permissions_types,
                "spatial_aggregation": aggregation_types,
            },
            "available_dates": {"permissions": {"get_result": True}},
        }
    )
    return all_claims


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
    return access_token_builder(get_all_claims_from_flowapi(flowapi_url=flowapi_url))


@click.group(chain=True)
@click.argument("username", type=str)
@click.argument("secret-key", type=str)
@click.argument("lifetime", type=int)
@click.argument("audience", type=str)
def print_token(username, secret_key, lifetime, audience):
    """
    Generate a JWT token for access to FlowAPI.

    Use the --all-access option to generate a token which allows full access to a
    specific FlowAPI server, or specify the queries you want the token to grant
    access to by providing multiple --query options.

    For example:

    generate-jwt TEST_USER SECRET 1 TEST_SERVER --all-access http://localhost:9090

    Or,

    generate-jwt TEST_USER SECRET 1 TEST_SERVER --query -a admin0 -a admin1 -p run -p get_result daily_location --query -a admin0 -p get_result flows
    """
    pass


@print_token.resultcallback()
def output_token(claims, username, secret_key, lifetime, audience):
    click.echo(
        generate_token(
            username=username,
            secret=secret_key,
            lifetime=datetime.timedelta(days=lifetime),
            claims=dict(ChainMap(*claims)),
            audience=audience,
        )
    )


@print_token.command("--query")
@click.argument("query_name", type=str)
@click.option(
    "--permission",
    "-p",
    type=click.Choice(permissions_types.keys()),
    multiple=True,
    help="Types of access allowed.",
)
@click.option(
    "--aggregation",
    "-a",
    type=click.Choice(aggregation_types),
    multiple=True,
    help="Spatial aggregation level of access allowed.",
)
def named_query(query_name, permission, aggregation):
    if len(permission) == 0 and len(aggregation) == 0:
        click.confirm(
            f"This will grant _no_ permissions for '{query_name}'. Are you sure?",
            abort=True,
        )
    return {
        query_name: {
            "permissions": {p: True for p in permission},
            "spatial_aggregation": aggregation,
        }
    }


@print_token.command("--all-access")
@click.argument("flowapi_url", type=str)
def print_all_access_token(flowapi_url):
    return get_all_claims_from_flowapi(flowapi_url=flowapi_url)
