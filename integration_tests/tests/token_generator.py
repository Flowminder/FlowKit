from datetime import timedelta
import click
from .utils import make_token, permissions_types, aggregation_types


@click.command()
@click.option("--username", default="test", help="Name of user to issue token for.")
@click.option(
    "--secret-key",
    prompt="JWT secret",
    help="JWT secret key to sign token with. Taken from the JWT_SECRET_KEY env var if available.",
    hide_input=True,
    envvar="JWT_SECRET_KEY",
)
@click.option("--lifetime", default=1, help="Days the token is valid for.")
@click.option(
    "--permission",
    "-p",
    type=(str, click.Choice(permissions_types)),
    multiple=True,
    help="Query kinds this token will allow access to, and type of access allowed.",
)
@click.option(
    "--aggregation",
    "-a",
    type=(str, click.Choice(aggregation_types)),
    multiple=True,
    help="Query kinds this token will allow access to, and spatial aggregation level of access allowed.",
)
def print_token(username, secret_key, lifetime, permission, aggregation):
    claims = {}
    for query_kind, permission_type in permission:
        if query_kind in claims.keys():
            claims[query_kind]["permissions"].append(permission_type)
        else:
            claims[query_kind] = {
                "permissions": [permission_type],
                "spatial_aggregation": [],
            }
    for query_kind, aggregation_type in aggregation:
        if query_kind in claims.keys():
            claims[query_kind]["spatial_aggregation"].append(aggregation_type)
        else:
            claims[query_kind] = {
                "permissions": [],
                "spatial_aggregation": [aggregation_type],
            }
    print(make_token(username, secret_key, timedelta(days=lifetime), claims))


if __name__ == "__main__":
    print_token()
