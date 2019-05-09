# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import base64

import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from flowkit_jwt_generator import generate_keypair
from flowkit_jwt_generator.jwt import load_private_key, load_public_key


def test_keypair_generator():
    """
    Keypair generator produces a valid key pair, and we can load it from a string.
    """
    private_key, public_key = generate_keypair()
    rsa_private_key = load_private_key(private_key.decode())
    assert private_key == rsa_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    rsa_public_key = load_public_key(public_key.decode())
    assert public_key == (
        rsa_public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )


def test_load_base64_encoded_private_key():
    """
    We can load a base64 encoded private key.
    """
    private_key, public_key = generate_keypair()
    rsa_private_key = load_private_key(base64.b64encode(private_key).decode())
    assert private_key == rsa_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )


def test_load_base64_encoded_public_key():
    """
    We can load a base64 encoded public key.
    """
    private_key, public_key = generate_keypair()
    rsa_public_key = load_public_key(base64.b64encode(public_key).decode())
    assert public_key == rsa_public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


@pytest.mark.parametrize("func", [load_public_key, load_public_key])
def test_bad_key_error(func):
    """
    Strings which aren't a key, or a base64 encoded key raise a valueerror.
    """
    with pytest.raises(ValueError):
        func("NOT_A_VALID_KEY")
