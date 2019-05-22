# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
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


def get_config():
    flowauth_fernet_key = get_secret_or_env_var("FLOWAUTH_FERNET_KEY").encode()
    _ = Fernet(flowauth_fernet_key)  # Error if fernet key is bad
    log_level = getattr(
        logging, os.getenv("FLOWAUTH_LOG_LEVEL", "error").upper(), logging.ERROR
    )
    return dict(
        LOG_LEVEL=log_level,
        ADMIN_USER=get_secret_or_env_var("FLOWAUTH_ADMIN_USER"),
        ADMIN_PASSWORD=get_secret_or_env_var("FLOWAUTH_ADMIN_PASSWORD"),
        SQLALCHEMY_DATABASE_URI=get_secret_or_env_var(
            "DB_URI", os.getenv("DB_URI", "sqlite:////tmp/test.db")
        ),
        SECRET_KEY=get_secret_or_env_var("SECRET_KEY"),
        SESSION_PROTECTION="strong",
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        FLOWAUTH_FERNET_KEY=flowauth_fernet_key,
        DEMO_MODE=True if os.getenv("DEMO_MODE") is not None else False,
        RESET_DB=True if os.getenv("RESET_FLOWAUTH_DB") is not None else False,
    )
