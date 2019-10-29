# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import base64
import datetime
import os
import uuid
import binascii
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


# Duplicated in FlowAuth (cannot use this implementation there because
# this module is outside the docker build context for FlowAuth).
def generate_token(
    *,
    flowapi_identifier: Optional[str] = None,
    username: str,
    private_key: Union[str, _RSAPrivateKey],
    lifetime: datetime.timedelta,
    claims: Dict[str, Dict[str, Union[Dict[str, bool], List[str]]]],
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
    claims : dict
        Dictionary of claims the token will grant
    flowapi_identifier : str, optional
        Optionally provide a string to identify the audience of the token

    Examples
    --------
    >>> generate_token(flowapi_identifier="TEST_SERVER",username="TEST_USER",private_key=rsa_private_key,lifetime=datetime.timedelta(5),claims={"daily_location":{"permissions": {"run":True},)
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
    if flowapi_identifier is not None:
        token_data["aud"] = flowapi_identifier
    return jwt.encode(
        payload=token_data, key=private_key, algorithm="RS256", json_encoder=JSONEncoder
    ).decode("utf-8")


def get_all_claims_from_flowapi(
    flowapi_url: str,
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


@click.group(chain=True)
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
def print_token(username, private_key, lifetime, audience):
    """
    Generate a JWT token for access to FlowAPI.

    Use the --all-access option to generate a token which allows full access to a
    specific FlowAPI server, or specify the queries you want the token to grant
    access to by providing multiple --query options.

    For example:

    \b
    generate-jwt --username TEST_USER --private-key $PRIVATE_JWT_SIGNING_KEY --lifetime 1 --audience TEST_SERVER all-access -u http://localhost:9090

    Or,

    \b
    generate-jwt --username TEST_USER --private-key $PRIVATE_JWT_SIGNING_KEY --lifetime 1 --audience TEST_SERVER query -a admin0 -a admin1 -p run -p get_result -q daily_location query -a admin0 -p get_result -q flows

    \b
    generate-jwt --username TEST_USER --private-key $PRIVATE_JWT_SIGNING_KEY --lifetime 1 --audience TEST_SERVER all-access -u http://localhost:9090

    Or,

    \b
    generate-jwt --username TEST_USER --private-key $PRIVATE_JWT_SIGNING_KEY --lifetime 1 --audience TEST_SERVER query -a admin0 -a admin1 -p run -p get_result -q daily_location query -a admin0 -p get_result -q flows
    """
    pass


@print_token.resultcallback()
def output_token(claims, username, private_key, lifetime, audience):
    click.echo(
        generate_token(
            flowapi_identifier=audience,
            username=username,
            private_key=load_private_key(private_key),
            lifetime=datetime.timedelta(days=lifetime),
            claims=dict(ChainMap(*claims)),
        )
    )


@print_token.command("query")
@click.option(
    "--query-name", "-q", type=str, help="Name of the query type.", required=True
)
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


@print_token.command("all-access")
@click.option(
    "--flowapi-url",
    "-u",
    type=str,
    required=True,
    help="URL of the FlowAPI server to grant access to.",
)
def print_all_access_token(flowapi_url):
    return get_all_claims_from_flowapi(flowapi_url=flowapi_url)
