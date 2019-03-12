# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import os
from pathlib import Path


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


def get_config():
    return dict(
        JWT_SECRET_KEY=getsecret("JWT_SECRET_KEY", os.getenv("JWT_SECRET_KEY")),
        LOG_DIRECTORY=os.getenv("LOG_DIRECTORY", "/var/log/flowapi/"),
        LOG_LEVEL=logging.getLevelName(os.getenv("LOG_LEVEL", "error").upper()),
        FLOWMACHINE_SERVER=os.getenv("FLOWMACHINE_SERVER", "localhost"),
        FLOWMACHINE_PORT=os.getenv("FLOWMACHINE_PORT", "5555"),
        FLOWDB_DSN=f'postgres://{getsecret("API_DB_USER", os.getenv("DB_USER"))}:{getsecret("API_DB_PASS", os.getenv("DB_PASS"))}@{os.getenv("DB_HOST")}:{os.getenv("FLOWDB_PORT", 5432)}/flowdb',
    )
