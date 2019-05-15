# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import base64
import binascii
import logging
import os
from pathlib import Path

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.backends.openssl.rsa import _RSAPublicKey
from cryptography.hazmat.primitives import serialization


def getsecret(key: str, default: str) -> str:
    """
    Get a value from docker secrets (i.e. read it from a file in
    /run/secrets), return a default if the file is not there.

    Parameters
    ----------
    key: str
        Name of the secret.
    default: str
        Default value to return if the file does not exist

    Returns
    -------
    str
        Value in the file, or default
    """
    try:
        with open(Path("/run/secrets") / key, "r") as fin:
            return fin.read().strip()
    except FileNotFoundError:
        return default


# Duplicated in flowkit_jwt_generator (cannot re-use the implementation
# there because the module is outside the docker build context for flowauth).
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
        except (binascii.Error, ValueError):
            raise ValueError("Failed to load key.")


def get_config():
    try:
        jwt_public_key = load_public_key(
            getsecret("PUBLIC_JWT_SIGNING_KEY", os.environ["PUBLIC_JWT_SIGNING_KEY"])
        )
    except (ValueError, KeyError):
        raise EnvironmentError("Could not load public key.")
    log_level = logging.getLevelName(os.getenv("FLOWAPI_LOG_LEVEL", "error").upper())

    flowmachine_host = os.environ["FLOWMACHINE_HOST"]
    flowmachine_port = os.environ["FLOWMACHINE_PORT"]

    flowdb_user = getsecret("FLOWAPI_FLOWDB_USER", os.environ["FLOWAPI_FLOWDB_USER"])
    flowdb_password = getsecret(
        "FLOWAPI_FLOWDB_PASSWORD", os.environ["FLOWAPI_FLOWDB_PASSWORD"]
    )
    flowdb_host = os.environ["FLOWDB_HOST"]
    flowdb_port = os.environ["FLOWDB_PORT"]
    flowapi_server_id = getsecret(
        "FLOWAPI_IDENTIFIER", os.environ["FLOWAPI_IDENTIFIER"]
    )

    return dict(
        JWT_PUBLIC_KEY=jwt_public_key,
        JWT_ALGORITHM="RS256",
        FLOWAPI_LOG_LEVEL=log_level,
        FLOWMACHINE_HOST=flowmachine_host,
        FLOWMACHINE_PORT=flowmachine_port,
        FLOWDB_DSN=f"postgres://{flowdb_user}:{flowdb_password}@{flowdb_host}:{flowdb_port}/flowdb",
        JWT_DECODE_AUDIENCE=flowapi_server_id,
    )
