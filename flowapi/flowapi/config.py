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
    jwt_secret_key = getsecret("JWT_SECRET_KEY", os.environ["JWT_SECRET_KEY"])
    log_level = logging.getLevelName(os.getenv("FLOWAPI_LOG_LEVEL", "error").upper())

    flowmachine_host = os.environ["FLOWMACHINE_HOST"]
    flowmachine_port = os.environ["FLOWMACHINE_PORT"]

    flowdb_user = getsecret("FLOWAPI_FLOWDB_USER", os.environ["FLOWAPI_FLOWDB_USER"])
    flowdb_password = getsecret(
        "FLOWAPI_FLOWDB_PASSWORD", os.environ["FLOWAPI_FLOWDB_PASSWORD"]
    )
    flowdb_host = os.environ["FLOWDB_HOST"]
    flowdb_port = os.environ["FLOWDB_PORT"]
    server_id = getsecret("FLOWAPI_IDENTIFIER", os.environ["FLOWAPI_IDENTIFIER"])

    return dict(
        JWT_SECRET_KEY=jwt_secret_key,
        FLOWAPI_LOG_LEVEL=log_level,
        FLOWMACHINE_HOST=flowmachine_host,
        FLOWMACHINE_PORT=flowmachine_port,
        FLOWDB_DSN=f"postgres://{flowdb_user}:{flowdb_password}@{flowdb_host}:{flowdb_port}/flowdb",
        JWT_DECODE_AUDIENCE=server_id,
    )
