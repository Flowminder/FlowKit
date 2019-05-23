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


class UndefinedConfigOption(Exception):
    """
    Indicates that a required configuration option was not provided.
    """


def get_secret_or_env_var(key: str) -> str:
    """
    Get a value from docker secrets (i.e. read it from a file in
    /run/secrets) or from the environment variable with the same
    name. Raises an error if neither is defined.

    Parameters
    ----------
    key: str
        Name of the secret / environment variable.

    Returns
    -------
    str
        Value in the file, or value of the environment variable.

    Raises
    ------
    UndefinedConfigOption
        If neither a docker secret nor an environment variable for the given key is defined.
    """
    try:
        with open(Path("/run/secrets") / key, "r") as fin:
            return fin.read().strip()
    except FileNotFoundError:
        try:
            return os.environ[key]
        except KeyError:
            raise UndefinedConfigOption(
                f"Undefined configuration option: '{key}'. Please set docker secret or environment variable."
            )


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

    jwt_public_key = load_public_key(get_secret_or_env_var("PUBLIC_JWT_SIGNING_KEY"))

    log_level = logging.getLevelName(os.getenv("FLOWAPI_LOG_LEVEL", "error").upper())

    flowmachine_host = get_secret_or_env_var("FLOWMACHINE_HOST")
    flowmachine_port = get_secret_or_env_var("FLOWMACHINE_PORT")

    flowdb_user = get_secret_or_env_var("FLOWAPI_FLOWDB_USER")
    flowdb_password = get_secret_or_env_var("FLOWAPI_FLOWDB_PASSWORD")
    flowdb_host = get_secret_or_env_var("FLOWDB_HOST")
    flowdb_port = get_secret_or_env_var("FLOWDB_PORT")
    flowapi_server_id = get_secret_or_env_var("FLOWAPI_IDENTIFIER")

    return dict(
        JWT_PUBLIC_KEY=jwt_public_key,
        JWT_ALGORITHM="RS256",
        FLOWAPI_LOG_LEVEL=log_level,
        FLOWMACHINE_HOST=flowmachine_host,
        FLOWMACHINE_PORT=flowmachine_port,
        FLOWDB_DSN=f"postgres://{flowdb_user}:{flowdb_password}@{flowdb_host}:{flowdb_port}/flowdb",
        JWT_DECODE_AUDIENCE=flowapi_server_id,
    )
