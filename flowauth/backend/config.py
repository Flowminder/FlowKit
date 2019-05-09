# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
from pathlib import Path
from cryptography.fernet import Fernet


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


SQLALCHEMY_DATABASE_URI = getsecret(
    "DB_URI", os.getenv("DB_URI", "sqlite:////tmp/test.db")
)
SECRET_KEY = getsecret("SECRET_KEY", os.getenv("SECRET_KEY", "secret"))
SESSION_PROTECTION = "strong"
SQLALCHEMY_TRACK_MODIFICATIONS = False
FLOWAUTH_FERNET_KEY = getsecret(
    "FLOWAUTH_FERNET_KEY", os.getenv("FLOWAUTH_FERNET_KEY", "")
).encode()
Fernet(FLOWAUTH_FERNET_KEY)  # Error if fernet key is bad
DEMO_MODE = True if os.getenv("DEMO_MODE") is not None else False
