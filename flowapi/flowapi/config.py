# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import os
from pathlib import Path


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
            raise UndefinedConfigOption(
                f"Undefined configuration option: '{key}'. Please set docker secret or environment variable."
            )


def get_config():
    jwt_secret_key = get_secret_or_env_var("JWT_SECRET_KEY")
    log_level = logging.getLevelName(os.getenv("FLOWAPI_LOG_LEVEL", "error").upper())

    flowmachine_host = get_secret_or_env_var("FLOWMACHINE_HOST")
    flowmachine_port = get_secret_or_env_var("FLOWMACHINE_PORT")

    flowdb_user = get_secret_or_env_var("FLOWAPI_FLOWDB_USER")
    flowdb_password = get_secret_or_env_var("FLOWAPI_FLOWDB_PASSWORD")
    flowdb_host = get_secret_or_env_var("FLOWDB_HOST")
    flowdb_port = get_secret_or_env_var("FLOWDB_PORT")
    flowapi_server_id = get_secret_or_env_var("FLOWAPI_IDENTIFIER")

    return dict(
        JWT_SECRET_KEY=jwt_secret_key,
        FLOWAPI_LOG_LEVEL=log_level,
        FLOWMACHINE_HOST=flowmachine_host,
        FLOWMACHINE_PORT=flowmachine_port,
        FLOWDB_DSN=f"postgres://{flowdb_user}:{flowdb_password}@{flowdb_host}:{flowdb_port}/flowdb",
        JWT_DECODE_AUDIENCE=flowapi_server_id,
    )
