# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import base64
import gzip
import io

import binascii
import datetime
import uuid

try:
    import simplejson as json
    from simplejson import JSONEncoder
except ImportError:
    try:
        import rapidjson as json
        from rapidjson import JSONEncoder
    except ImportError:
        import json
        from json import JSONEncoder

from typing import Iterable, List, Optional, Tuple, Union

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.backends.openssl.rsa import _RSAPrivateKey, _RSAPublicKey
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from functools import lru_cache

import jwt


def compress_claims(claims):
    out = io.BytesIO()

    with gzip.open(out, mode="wt") as fo:
        json.dump(claims, fo)

    return base64.encodebytes(out.getvalue()).decode()


def decompress_claims(claims):
    in_ = io.BytesIO()
    in_.write(base64.decodebytes(claims.encode()))
    in_.seek(0)
    with gzip.GzipFile(fileobj=in_, mode="rb") as fo:
        return json.load(fo)


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


def squash(xs, ix=0):
    """
    Squashes a list of scope strings by combining them where possible.

    Given two strings of the form <action>&<query_kind>.<arg_name>.<arg_val> that differ only
    in <action>, will yield <action_a>,<action_b>&<query_kind>.<arg_name>.<arg_val>.

    Repeatedly squashes, then if ix < the greatest number of & separated elements in the result, squashes again
    using the next & separated element.

    Parameters
    ----------
    xs : list of str
        List of scope strings of the form <action>&<query_kind>.<arg_name>.<arg_val>
    ix : int, default 0


    Yields
    ------
    str
        Merged scope string

    """
    sq = {}
    can_squash = False
    for x in xs:
        s = x.split("&")[ix + 1 :]
        hs = x.split("&")[:ix]
        dd = sq.setdefault(("&".join(hs), "&".join(s)), dict())
        ll = dd.setdefault("h", set())
        try:
            h = x.split("&")[ix]
            ll.add(h)
            can_squash = True
        except IndexError:
            pass

    ll = set()
    for k, v in sq.items():
        parts = []
        if len(k[0]) > 0:
            parts.append(k[0])
        if len(v["h"]) > 0:
            parts.append(",".join(sorted(v["h"])))
        if len(k[1]) > 0:
            parts.append(k[1])
        ll.add("&".join(parts))
    res = list(sorted(ll))
    if can_squash:
        return squash(res, ix + 1)
    return res


def squashed_scopes(scopes: List[str]) -> Iterable[str]:
    """
    Squashes a list of scope strings by combining them where possible.

    Given two strings of the form <action>&<query_kind>.<arg_name>.<arg_val> that differ only
    in <action>, will yield <action_a>,<action_b>&<query_kind>.<arg_name>.<arg_val>.

    Parameters
    ----------
    scopes : list of str
        List of scope strings of the form <action>&<query_kind>.<arg_name>.<arg_val>

    Yields
    ------
    str
        Merged scope string

    """
    yield from squash(scopes)


# Duplicated in FlowAuth (cannot use this implementation there because
# this module is outside the docker build context for FlowAuth).
def generate_token(
    *,
    flowapi_identifier: Optional[str] = None,
    flowauth_identifier: Optional[str] = None,
    username: str,
    private_key: Union[str, _RSAPrivateKey],
    lifetime: datetime.timedelta,
    roles: dict,
    compress: bool = True,
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
    roles : dict
        Dict of claims this token will contain in the form {role_name:[scope_list], ...}
    flowapi_identifier : str, optional
        Optionally provide a string to identify the audience of the token
    compress : bool
        Set to False to disable compressing claims with gzip.

    Examples
    --------
    >>> generate_token(flowapi_identifier="TEST_SERVER",username="TEST_USER",private_key=rsa_private_key,lifetime=datetime.timedelta(5),roles={"read_role":["get_results"]})
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
        user_claims=compress_claims(roles) if compress else roles,
        sub=username,
        exp=now + lifetime,
    )
    if flowapi_identifier is not None:
        token_data["aud"] = flowapi_identifier
    if flowauth_identifier is not None:
        token_data["iss"] = flowauth_identifier
    return jwt.encode(
        payload=token_data, key=private_key, algorithm="RS256", json_encoder=JSONEncoder
    )


@lru_cache(None)
def get_security_schemes_from_api_spec(flowapi_url: str) -> dict:
    """
    Get the security schemes section of an api spec from a flowapi server.

    Parameters
    ----------
    flowapi_url : str
        URL of the flowapi instance

    Returns
    -------
    dict
        The security schemes section

    """
    import requests

    return requests.get(f"{flowapi_url}/api/0/spec/openapi.json").json()["components"][
        "securitySchemes"
    ]


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
    return get_security_schemes_from_api_spec(flowapi_url)["token"]["x-security-scopes"]


def get_audience_from_flowapi(flowapi_url: str) -> str:
    """
    Get the audience name from a flowapi server.

    Parameters
    ----------
    flowapi_url : str
        Address of the flowapi server

    Returns
    -------
    str
        The audience name

    """
    return get_security_schemes_from_api_spec(flowapi_url)["token"]["x-audience"]
