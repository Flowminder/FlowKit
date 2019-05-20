# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import base64
from pathlib import Path
from binascii import Error
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.backends.openssl.rsa import _RSAPrivateKey
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


def get_config():
    SQLALCHEMY_DATABASE_URI = getsecret(
        "DB_URI", os.getenv("DB_URI", "sqlite:////tmp/test.db")
    )
    try:
        sqlalchemy_username = getsecret(
            "FLOWAUTH_DB_PASSWORD", os.environ["FLOWAUTH_DB_PASSWORD"]
        )
        SQLALCHEMY_DATABASE_URI = SQLALCHEMY_DATABASE_URI.format(sqlalchemy_username)
    except KeyError:
        pass  # No password

    SESSION_PROTECTION = "strong"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    FLOWAUTH_FERNET_KEY = getsecret(
        "FLOWAUTH_FERNET_KEY", os.getenv("FLOWAUTH_FERNET_KEY", "")
    ).encode()
    Fernet(FLOWAUTH_FERNET_KEY)  # Error if fernet key is bad
    DEMO_MODE = True if os.getenv("DEMO_MODE") is not None else False
    try:
        PRIVATE_JWT_SIGNING_KEY = load_private_key(
            getsecret("PRIVATE_JWT_SIGNING_KEY", os.environ["PRIVATE_JWT_SIGNING_KEY"])
        )
    except (ValueError, KeyError):
        raise EnvironmentError("Could not load private key.")

    return dict(
        SQLALCHEMY_DATABASE_URI=SQLALCHEMY_DATABASE_URI,
        SESSION_PROTECTION=SESSION_PROTECTION,
        SQLALCHEMY_TRACK_MODIFICATIONS=SQLALCHEMY_TRACK_MODIFICATIONS,
        FLOWAUTH_FERNET_KEY=FLOWAUTH_FERNET_KEY,
        DEMO_MODE=DEMO_MODE,
        PRIVATE_JWT_SIGNING_KEY=PRIVATE_JWT_SIGNING_KEY,
        PUBLIC_JWT_SIGNING_KEY=PRIVATE_JWT_SIGNING_KEY.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ),
    )
