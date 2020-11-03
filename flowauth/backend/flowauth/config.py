# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import binhex

import base64

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.backends.openssl.rsa import _RSAPrivateKey
from cryptography.hazmat.primitives import serialization
from dogpile.cache import make_region, CacheRegion
from multiprocessing import Event

from get_secret_or_env_var import environ, getenv

import logging

import os
from cryptography.fernet import Fernet


class UndefinedConfigOption(Exception):
    """
    Indicates that a required configuration option was not provided.
    """


def get_cache_backend() -> CacheRegion:
    """
    Get a dogpilecache cache region.

    Returns
    -------
    CacheRegion
    """
    cache_backend = getenv("FLOWAUTH_CACHE_BACKEND", "FILE").upper()

    if cache_backend == "REDIS":
        backend = "dogpile.cache.redis"
        cache_args = dict(
            host=environ["FLOWAUTH_REDIS_HOST"],
            port=int(getenv("FLOWAUTH_REDIS_PORT", "6379")),
            db=int(getenv("FLOWAUTH_REDIS_DB", "0")),
            redis_expiration_time=32,
            distributed_lock=True,
            password=getenv("FLOWAUTH_REDIS_PASSWORD", None),
        )
    elif cache_backend == "FILE":
        backend = "dogpile.cache.dbm"
        cache_args = dict(filename=environ["FLOWAUTH_CACHE_FILE"])
    else:
        backend = "dogpile.cache.memory"
        cache_args = {}

    return make_region().configure(
        backend=backend, expiration_time=30, arguments=cache_args
    )


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
    try:
        flowauth_fernet_key = environ["FLOWAUTH_FERNET_KEY"].encode()
        _ = Fernet(flowauth_fernet_key)  # Error if fernet key is bad
        log_level = getattr(
            logging, getenv("FLOWAUTH_LOG_LEVEL", "error").upper(), logging.ERROR
        )
        db_uri = getenv("DB_URI", os.getenv("DB_URI", "sqlite:////tmp/test.db"))
        db_uri = db_uri.format(getenv("FLOWAUTH_DB_PASSWORD", ""))

        return dict(
            PRIVATE_JWT_SIGNING_KEY=load_private_key(
                environ["PRIVATE_JWT_SIGNING_KEY"]
            ),
            LOG_LEVEL=log_level,
            ADMIN_USER=environ["FLOWAUTH_ADMIN_USERNAME"],
            ADMIN_PASSWORD=environ["FLOWAUTH_ADMIN_PASSWORD"],
            SQLALCHEMY_DATABASE_URI=db_uri,
            SQLALCHEMY_ENGINE_OPTIONS=dict(pool_recycle=3600),
            SECRET_KEY=environ["SECRET_KEY"],
            SESSION_PROTECTION="strong",
            SQLALCHEMY_TRACK_MODIFICATIONS=False,
            FLOWAUTH_FERNET_KEY=flowauth_fernet_key,
            DEMO_MODE=True if getenv("DEMO_MODE") is not None else False,
            RESET_DB=True if getenv("RESET_FLOWAUTH_DB") is not None else False,
            DB_IS_SET_UP=Event(),
            CACHE_BACKEND=get_cache_backend(),
            FLOWAUTH_TWO_FACTOR_ISSUER=getenv("FLOWAUTH_TWO_FACTOR_ISSUER", "FlowAuth"),
            TWO_FACTOR_VALID_WINDOW=int(getenv("TWO_FACTOR_VALID_WINDOW", 0)),
        )
    except KeyError as e:
        raise UndefinedConfigOption(
            f"Undefined configuration option: '{e.args[0]}'. Please set docker secret or environment variable."
        )
