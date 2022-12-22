# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime

import click

from flowkit_jwt_generator import (
    generate_token,
    load_private_key,
    get_all_claims_from_flowapi,
)
from flowkit_jwt_generator.jwt import get_audience_from_flowapi


@click.command()
@click.option("--username", type=str, required=True, help="Username this token is for.")
@click.option(
    "--private-key",
    type=str,
    envvar="PRIVATE_JWT_SIGNING_KEY",
    required=True,
    help="RSA private key, optionally base64 encoded.",
)
@click.option(
    "--lifetime", type=int, required=True, help="Lifetime in days of this token."
)
@click.option(
    "--flowapi-url",
    "-u",
    type=str,
    required=True,
    help="URL of the FlowAPI server to grant access to.",
)
def print_token(username, private_key, lifetime, flowapi_url):
    """
    Generate a JWT token for access to FlowAPI.

    For example:

    \b
    generate-jwt --username TEST_USER --private-key $PRIVATE_JWT_SIGNING_KEY --lifetime 1 -u http://localhost:9090
    """
    click.echo(
        generate_token(
            flowapi_identifier=get_audience_from_flowapi(flowapi_url=flowapi_url),
            username=username,
            private_key=load_private_key(private_key),
            lifetime=datetime.timedelta(days=lifetime),
            roles=dict(
                universal_role=get_all_claims_from_flowapi(flowapi_url=flowapi_url)
            ),
        )
    )
