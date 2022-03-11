# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import base64
import binascii
import logging


from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.backends.openssl.rsa import _RSAPublicKey
from cryptography.hazmat.primitives import serialization
from get_secret_or_env_var import environ, getenv


class UndefinedConfigOption(Exception):
    """
    Indicates that a required configuration option was not provided.
    """


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
            raise ValueError("Failed to load public key.")


def get_config():

    try:
        jwt_public_key = load_public_key(environ["PUBLIC_JWT_SIGNING_KEY"])

        log_level = logging.getLevelName(getenv("FLOWAPI_LOG_LEVEL", "error").upper())

        flowmachine_host = environ["FLOWMACHINE_HOST"]
        flowmachine_port = environ["FLOWMACHINE_PORT"]

        flowdb_user = environ["FLOWAPI_FLOWDB_USER"]
        flowdb_password = environ["FLOWAPI_FLOWDB_PASSWORD"]
        flowdb_host = environ["FLOWDB_HOST"]
        flowdb_port = environ["FLOWDB_PORT"]
        flowapi_server_id = environ["FLOWAPI_IDENTIFIER"]
    except KeyError as e:
        raise UndefinedConfigOption(
            f"Undefined configuration option: '{e.args[0]}'. Please set docker secret or environment variable."
        )

    return dict(
        JWT_PUBLIC_KEY=jwt_public_key,
        JWT_ALGORITHM="RS256",
        FLOWAPI_LOG_LEVEL=log_level,
        FLOWMACHINE_HOST=flowmachine_host,
        FLOWMACHINE_PORT=flowmachine_port,
        FLOWDB_DSN=f"postgresql://{flowdb_user}:{flowdb_password}@{flowdb_host}:{flowdb_port}/flowdb",
        JWT_DECODE_AUDIENCE=flowapi_server_id,
    )
