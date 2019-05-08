# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
import os
import uuid
from json import JSONEncoder
from typing import Dict, List, Union, Callable
from datetime import timedelta
import click
import jwt
import pytest
import requests


permissions_types = {"run": True, "poll": True, "get_result": True}
aggregation_types = ["admin0", "admin1", "admin2", "admin3", "admin4"]


def generate_token(
    username: str,
    secret: str,
    lifetime: datetime.timedelta,
    claims: Dict[str, Dict[str, Union[Dict[str, bool], List[str]]]],
) -> str:
    """

    Parameters
    ----------
    username : str
        Username for the token
    secret : str
        Shared secret to sign the token with
    lifetime : datetime.timedelta
        Lifetime from now of the token
    claims : dict
        Dictionary of claims the token will grant

    Examples
    --------
    >>> generate_token("TEST_USER", "SECRET", datetime.timedelta(5), {"daily_location":{"permissions": {"run":True},
            "spatial_aggregation": ["admin3"}})
    'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1NTczMDMyOTMsIm5iZiI6MTU1NzMwMzI5MywianRpIjoiOTRjZWMxODQtZTc4Mi00ZDk3LTkxNDYtNTczZjlkMTBlNDI2IiwidXNlcl9jbGFpbXMiOnsiZGFpbHlfbG9jYXRpb24iOnsicGVybWlzc2lvbnMiOnsicnVuIjp0cnVlfSwic3BhdGlhbF9hZ2dyZWdhdGlvbiI6WyJhZG1pbjMiXX19LCJpZGVudGl0eSI6IlRFU1RfVVNFUiIsImV4cCI6MTU1NzczNTI5M30.CG7xlsyaKywK4lHpnlpI-DlNk4wdMNufrguz4Y6qDi4'

    Returns
    -------
    str
        Encoded token

    """

    now = datetime.datetime.utcnow()
    token_data = dict(
        iat=now,
        nbf=now,
        jti=str(uuid.uuid4()),
        user_claims=claims,
        identity=username,
        exp=now + lifetime,
    )
    return jwt.encode(
        payload=token_data, key=secret, algorithm="HS256", json_encoder=JSONEncoder
    ).decode("utf-8")


@pytest.fixture
def access_token_builder() -> Callable:
    """
    Fixture which builds short-life access tokens.

    Returns
    -------
    function
        Function which returns a token encoding the specified claims.
    """

    secret = os.getenv("JWT_SECRET_KEY")
    if secret is None:
        raise EnvironmentError("JWT_SECRET_KEY environment variable not set.")

    def token_maker(
        claims: Dict[str, Dict[str, Union[Dict[str, bool], List[str]]]]
    ) -> str:
        return generate_token(
            username="test",
            secret=secret,
            lifetime=timedelta(seconds=90),
            claims=claims,
        )

    return token_maker


def get_all_claims_from_flowapi(
    flowapi_url: str
) -> Dict[str, Dict[str, Dict[str, Union[Dict[str, bool], List[str]]]]]:
    """
    Retrieve all the query types available on an instance of flowapi and
    generate a claims dictionary which grants total access to them.

    Parameters
    ----------
    flowapi_url : str
        URL of the flowapi instance

    Returns
    -------
    dict
        Claims dictionary

    """
    query_kinds = []
    aggregation_types = set()
    spec = requests.get(f"{flowapi_url}/api/0/spec/openapi.json").json()
    for q_class, q_spec in spec["components"]["schemas"].items():
        try:
            query_kinds.append(q_spec["properties"]["query_kind"]["enum"][0])
            aggregation_types.update(q_spec["properties"]["aggregation_unit"]["enum"])
        except KeyError:
            pass
    query_kinds = sorted(query_kinds)
    aggregation_types = sorted(aggregation_types)
    all_claims = {
        query_kind: {
            "permissions": permissions_types,
            "spatial_aggregation": aggregation_types,
        }
        for query_kind in query_kinds
    }
    all_claims.update(
        {
            "geography": {
                "permissions": permissions_types,
                "spatial_aggregation": aggregation_types,
            },
            "available_dates": {"permissions": {"get_result": True}},
        }
    )
    return all_claims


@pytest.fixture
def universal_access_token(flowapi_url: str, access_token_builder: Callable) -> str:
    """

    Parameters
    ----------
    flowapi_url : str
    access_token_builder : pytest.fixture

    Returns
    -------
    str
        The token

    """
    return access_token_builder(get_all_claims_from_flowapi(flowapi_url=flowapi_url))


@click.command()
@click.option("--username", default="test", help="Name of user to issue token for.")
@click.option("--secret-key", default="secret", help="Shared secret.")
@click.option("--lifetime", default=1, help="Days the token is valid for.")
@click.option(
    "--permission",
    "-p",
    type=(str, click.Choice(permissions_types.keys())),
    multiple=True,
    help="Query kinds this token will allow access to, and type of access allowed, e.g. -p daily_location run",
)
@click.option(
    "--aggregation",
    "-a",
    type=(str, click.Choice(aggregation_types)),
    multiple=True,
    help="Query kinds this token will allow access to, and spatial aggregation level of access allowed, e.g. -a daily_location admin3",
)
def print_token(username, secret_key, lifetime, permission, aggregation):
    claims = {}
    for query_kind, permission_type in permission:
        if query_kind in claims.keys():
            claims[query_kind]["permissions"][permission_type] = True
        else:
            claims[query_kind] = {
                "permissions": {permission_type: True},
                "spatial_aggregation": [],
            }
    for query_kind, aggregation_type in aggregation:
        if query_kind in claims.keys():
            claims[query_kind]["spatial_aggregation"].append(aggregation_type)
        else:
            claims[query_kind] = {
                "permissions": {},
                "spatial_aggregation": [aggregation_type],
            }
    print(
        generate_token(
            username=username,
            secret=secret_key,
            lifetime=timedelta(days=lifetime),
            claims=claims,
        )
    )


@click.command()
@click.option("--username", default="test", help="Name of user to issue token for.")
@click.option("--secret-key", default="secret", help="Shared secret.")
@click.option("--lifetime", default=1, help="Days the token is valid for.")
@click.option(
    "--flowapi-url",
    default="http://localhost:9090",
    help="FlowAPI instance to generate an all access token for.",
)
def print_all_access_token(username, secret_key, lifetime, flowapi_url):
    print(
        generate_token(
            username=username,
            secret=secret_key,
            lifetime=timedelta(days=lifetime),
            claims=get_all_claims_from_flowapi(flowapi_url=flowapi_url),
        )
    )
