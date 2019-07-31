# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import binhex

import base64

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.backends.openssl.rsa import _RSAPrivateKey
from cryptography.hazmat.primitives import serialization
from multiprocessing import Event

from typing import Optional

import logging

import os
from pathlib import Path
from cryptography.fernet import Fernet


class UndefinedConfigOption(Exception):
    """
    Indicates that a required configuration option was not provided.
    """


def get_secret_or_env_var(key: str, default: Optional[str] = None) -> str:
    """
    Get a value from docker secrets (i.e. read it from a file in
    /run/secrets) or from the environment variable with the same
    name. Raises an error if neither is defined.
    Parameters
    ----------
    key : str
        Name of the secret / environment variable.
    default : str, optional
        Optionally return a default if neither is set.

    Returns
    -------
    str
        Value in the file, or value of the environment variable, or default if defined.
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
            if default is None:
                raise UndefinedConfigOption(
                    f"Undefined configuration option: '{key}'. Please set docker secret or environment variable."
                )
            else:
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
        except (binhex.Error, ValueError):
            raise ValueError("Failed to load private key.")


def get_config():
    flowauth_fernet_key = get_secret_or_env_var("FLOWAUTH_FERNET_KEY").encode()
    _ = Fernet(flowauth_fernet_key)  # Error if fernet key is bad
    log_level = getattr(
        logging, os.getenv("FLOWAUTH_LOG_LEVEL", "error").upper(), logging.ERROR
    )
    db_uri = get_secret_or_env_var(
        "DB_URI", os.getenv("DB_URI", "sqlite:////tmp/test.db")
    )
    db_uri = db_uri.format(get_secret_or_env_var("FLOWAUTH_DB_PASSWORD", ""))

    return dict(
        PRIVATE_JWT_SIGNING_KEY=load_private_key(
            get_secret_or_env_var("PRIVATE_JWT_SIGNING_KEY")
        ),
        LOG_LEVEL=log_level,
        ADMIN_USER=get_secret_or_env_var("FLOWAUTH_ADMIN_USERNAME"),
        ADMIN_PASSWORD=get_secret_or_env_var("FLOWAUTH_ADMIN_PASSWORD"),
        SQLALCHEMY_DATABASE_URI=db_uri,
        SECRET_KEY=get_secret_or_env_var("SECRET_KEY"),
        SESSION_PROTECTION="strong",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        FLOWAUTH_FERNET_KEY=flowauth_fernet_key,
        DEMO_MODE=True if os.getenv("DEMO_MODE") is not None else False,
        RESET_DB=True if os.getenv("RESET_FLOWAUTH_DB") is not None else False,
        DB_IS_SETTING_UP=Event(),
        DB_IS_SET_UP=Event(),
    )
