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
    jwt_secret_key = getsecret("JWT_SECRET_KEY", os.getenv("JWT_SECRET_KEY"))
    log_directory = os.getenv("LOG_DIRECTORY", "/var/log/flowapi/")
    log_level = logging.getLevelName(os.getenv("LOG_LEVEL", "error").upper())

    flowmachine_server = os.getenv("FLOWMACHINE_SERVER", "localhost")
    flowmachine_port = os.getenv("FLOWMACHINE_PORT", "5555")

    flowdb_user = getsecret("API_FLOWDB_USER", os.getenv("FLOWDB_USER"))
    flowdb_password = getsecret("FLOWAPI_DB_PASS", os.getenv("FLOWDB_PASS"))
    flowdb_host = os.getenv("FLOWDB_HOST")
    flowdb_port = os.getenv("FLOWDB_PORT", 5432)

    return dict(
        JWT_SECRET_KEY=jwt_secret_key,
        LOG_DIRECTORY=log_directory,
        LOG_LEVEL=log_level,
        FLOWMACHINE_SERVER=flowmachine_server,
        FLOWMACHINE_PORT=flowmachine_port,
        FLOWDB_DSN=f"postgres://{flowdb_user}:{flowdb_password}@{flowdb_host}:{flowdb_port}/flowdb",
    )
