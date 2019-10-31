# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import base64
import datetime

import uuid
import binascii
from itertools import takewhile
from json import JSONEncoder
from typing import List, Union, Tuple, Optional, Iterable
import click
import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.backends.openssl.rsa import _RSAPrivateKey, _RSAPublicKey
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


# Duplicated in FlowAuth (cannot use this implementation there because
# this module is outside the docker build context for FlowAuth).
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
        except (binascii.Error, ValueError):
            raise ValueError("Failed to load key.")


# Duplicated in FlowAPI (cannot use this implementation there because
# this module is outside the docker build context for FlowAuth).
def load_public_key(key_string: str) -> _RSAPublicKey:
    """
    Load a public key from a string, which may be base64 encoded.

    Parameters
    ----------
    key_string : str
        String containing the key, optionally base64 encoded

    Returns
    -------
    _RSAPublicKey
        The public key
    """

    try:
        return serialization.load_pem_public_key(
            key_string.encode(), backend=default_backend()
        )
    except (ValueError, TypeError):
        try:
            return load_public_key(base64.b64decode(key_string).decode())
        except (binascii.Error, ValueError):
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


def squash(xs):
    sq = {}
    for x in xs:
        h, *s = x.split(":")
        ll = sq.setdefault(h, list())
        if len(s) > 0:
            ll.append(":".join(s))

    sq2 = {}

    while len(sq) > 0:
        sqit = iter(sq.keys())
        k = next(sqit)
        v = sq.pop(k)
        ks = [
            k2
            for k2, v2 in sq.items()
            if isinstance(v2, list) and k2 != k and sorted(v2) == sorted(v)
        ]
        if len(ks) > 0:
            sq2[",".join(sorted([k, *ks]))] = v
        else:
            sq2[k] = v
        for k in ks:
            sq.pop(k)

    for k, v in sq2.items():
        if isinstance(v, list) and len(v) > 1:
            sq2[k] = squash(v)

    return sq2


def compose_scope(p):
    for k, v in p.items():
        if isinstance(v, dict) and len(v) > 0:
            yield f"{k}:{':'.join(compose_scope(v))}"
        elif isinstance(v, list) and len(v) > 0:
            yield f"{k}:{v[0]}"
        else:
            yield k


def squashed_scopes(scopes: List[str]) -> Iterable[str]:
    """

    Parameters
    ----------
    scopes

    Returns
    -------

    """
    yield from compose_scope(squash(scopes))


# Duplicated in FlowAuth (cannot use this implementation there because
# this module is outside the docker build context for FlowAuth).
def generate_token(
    *,
    flowapi_identifier: Optional[str] = None,
    username: str,
    private_key: Union[str, _RSAPrivateKey],
    lifetime: datetime.timedelta,
    claims: List[str],
) -> str:
    """

    Parameters
    ----------
    username : str
        Username for the token
    private_key : str or _RSAPrivateKey
        Private key to use to sign the token.  May be an _RSAPrivateKey object, or a string
        containing a PEM encoded key
    lifetime : datetime.timedelta
        Lifetime from now of the token
    claims : list
        List of claims this token will contain
    flowapi_identifier : str, optional
        Optionally provide a string to identify the audience of the token

    Examples
    --------
    >>> generate_token(flowapi_identifier="TEST_SERVER",username="TEST_USER",private_key=rsa_private_key,lifetime=datetime.timedelta(5),claims=["run:spatial_aggregate:locations:daily_location:aggregation_unit:admin3"])
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
        user_claims=list(squashed_scopes(claims)),
        identity=username,
        exp=now + lifetime,
    )
    if flowapi_identifier is not None:
        token_data["aud"] = flowapi_identifier
    return jwt.encode(
        payload=token_data, key=private_key, algorithm="RS256", json_encoder=JSONEncoder
    ).decode("utf-8")


def get_all_claims_from_flowapi(flowapi_url: str) -> List[str]:
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
    spec = requests.get(f"{flowapi_url}/api/0/spec/openapi.json").json()
    return spec["components"]["securitySchemes"]["token"]["x-security-scopes"]


@click.command()
@click.option("--username", type=str, required=True, help="Username this token is for.")
@click.option(
    "--private-key",
    type=str,
    envvar="PRIVATE_JWT_SIGNING_KEY",
    required=True,
    help="RSA private key, optionally base64 encoded.",
)
@click.option(
    "--lifetime", type=int, required=True, help="Lifetime in days of this token."
)
@click.option(
    "--audience",
    type=str,
    required=True,
    help="FlowAPI server this token may be used with.",
)
@click.option(
    "--flowapi-url",
    "-u",
    type=str,
    required=True,
    help="URL of the FlowAPI server to grant access to.",
)
def print_token(username, private_key, lifetime, audience, flowapi_url):
    """
    Generate a JWT token for access to FlowAPI.

    For example:

    \b
    generate-jwt --username TEST_USER --private-key $PRIVATE_JWT_SIGNING_KEY --lifetime 1 --audience TEST_SERVER -u http://localhost:9090
    """
    click.echo(
        generate_token(
            flowapi_identifier=audience,
            username=username,
            private_key=load_private_key(private_key),
            lifetime=datetime.timedelta(days=lifetime),
            claims=get_all_claims_from_flowapi(flowapi_url=flowapi_url),
        )
    )
