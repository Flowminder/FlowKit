# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
from pathlib import Path
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
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


def get_config():
    SQLALCHEMY_DATABASE_URI = getsecret(
        "DB_URI", os.getenv("DB_URI", "sqlite:////tmp/test.db")
    )
    SECRET_KEY = getsecret("SECRET_KEY", os.getenv("SECRET_KEY", "secret"))
    SESSION_PROTECTION = "strong"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    FERNET_KEY = getsecret("FERNET_KEY", os.getenv("FERNET_KEY", "")).encode()
    Fernet(FERNET_KEY)  # Error if fernet key is bad
    DEMO_MODE = True if os.getenv("DEMO_MODE") is not None else False
    PRIVATE_JWT_SIGNING_KEY = getsecret(
        "PRIVATE_JWT_SIGNING_KEY", os.getenv("PRIVATE_JWT_SIGNING_KEY")
    )
    PUBLIC_JWT_SIGNING_KEY = getsecret(
        "PUBLIC_JWT_SIGNING_KEY", os.getenv("PUBLIC_JWT_SIGNING_KEY")
    )
    if PRIVATE_JWT_SIGNING_KEY is None:
        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )
        PRIVATE_JWT_SIGNING_KEY = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        PUBLIC_JWT_SIGNING_KEY = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

    return dict(
        SQLALCHEMY_DATABASE_URI=SQLALCHEMY_DATABASE_URI,
        SECRET_KEY=SECRET_KEY,
        SESSION_PROTECTION=SESSION_PROTECTION,
        SQLALCHEMY_TRACK_MODIFICATIONS=SQLALCHEMY_TRACK_MODIFICATIONS,
        FERNET_KEY=FERNET_KEY,
        DEMO_MODE=DEMO_MODE,
        PRIVATE_JWT_SIGNING_KEY=PRIVATE_JWT_SIGNING_KEY,
        PUBLIC_JWT_SIGNING_KEY=PUBLIC_JWT_SIGNING_KEY,
    )
