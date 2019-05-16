# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
from unittest.mock import Mock

import pytest
from cryptography.hazmat.backends.openssl.rsa import _RSAPublicKey, _RSAPrivateKey
from cryptography.hazmat.primitives import serialization

from flowkit_jwt_generator import load_private_key, load_public_key

pytest_plugins = ["pytester"]


@pytest.fixture
def private_key() -> _RSAPrivateKey:
    return load_private_key(os.environ["PRIVATE_JWT_SIGNING_KEY"])


@pytest.fixture
def public_key() -> _RSAPublicKey:
    return load_public_key(os.environ["PUBLIC_JWT_SIGNING_KEY"])


@pytest.fixture
def public_key_bytes(public_key) -> bytes:
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


@pytest.fixture
def dummy_flowapi(monkeypatch):
    """
    Fixture which monkeypatches requests.get to return a dummy flowapi schema and returns
    the expected decoded user claims dict.
    """
    get_mock = Mock()
    get_mock.return_value.json.return_value = {
        "components": {
            "schemas": {
                "DUMMY_QUERY": {
                    "properties": {
                        "query_kind": {"enum": ["DUMMY_QUERY_KIND"]},
                        "aggregation_unit": {
                            "enum": ["admin0", "admin1", "admin2", "admin3"],
                            "type": "string",
                        },
                    }
                },
                "NOT_A_DUMMY_QUERY": {
                    "properties": {
                        "aggregation_unit": {
                            "enum": ["admin0", "admin1", "admin2", "admin3"],
                            "type": "string",
                        }
                    }
                },
                "DUMMY_QUERY_WITH_NO_AGGREGATIONS": {
                    "properties": {
                        "query_kind": {
                            "enum": ["DUMMY_QUERY_WITH_NO_AGGREGATIONS_KIND"]
                        }
                    }
                },
            }
        }
    }
    monkeypatch.setattr("requests.get", get_mock)
    return {
        "DUMMY_QUERY_KIND": {
            "permissions": {"get_result": True, "poll": True, "run": True},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
        "DUMMY_QUERY_WITH_NO_AGGREGATIONS_KIND": {
            "permissions": {"get_result": True, "poll": True, "run": True},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
        "available_dates": {"permissions": {"get_result": True}},
        "geography": {
            "permissions": {"get_result": True, "poll": True, "run": True},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
    }
