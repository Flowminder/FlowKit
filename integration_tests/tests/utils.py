"""
Duplicate of flowapi/tests/unit/utils.py
"""
# TODO: De-duplicate this
import datetime
import json
import uuid

import jwt
from flowmachine.core.server.query_schemas import FlowmachineQuerySchema
from flowmachine.core.server.query_schemas.spatial_aggregate import (
    InputToSpatialAggregate,
)
from flowmachine.core.server.query_schemas.joined_spatial_aggregate import (
    JoinableMetrics,
)


def make_token(username, secret_key, private_key, lifetime, claims):
    """
    Produce a JWT for access to the API.
    Parameters
    ----------
    username : str
        Name of user to issue token for.
    secret_key : str
        Secret indicating the audience for this token.
    private_key : str
        RSA private key for signing the token
    lifetime : timedelta
        Time for which the token is valid.
    claims : dict
        User claims. Query types this token will allow access to, and type of access allowed.
    """
    now = datetime.datetime.utcnow()
    token_data = dict(
        iat=now,
        nbf=now,
        jti=str(uuid.uuid4()),
        user_claims=claims,
        aud=secret_key,
        identity=username,
        exp=now + lifetime,
    )
    return jwt.encode(
        payload=token_data,
        key=,
        algorithm="RS256",
        json_encoder=json.JSONEncoder,
    ).decode("utf-8")


query_kinds = (
    list(FlowmachineQuerySchema.type_schemas.keys())
    + list(InputToSpatialAggregate.type_schemas.keys())
    + list(JoinableMetrics.type_schemas.keys())
)
permissions_types = {"run": True, "poll": True, "get_result": True}
aggregation_types = ["admin0", "admin1", "admin2", "admin3", "admin4"]
all_access_claims = {
    query_kind: {
        "permissions": permissions_types,
        "spatial_aggregation": aggregation_types,
    }
    for query_kind in query_kinds
}
