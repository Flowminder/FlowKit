"""
Duplicate of flowapi/tests/unit/utils.py
"""
# TODO: De-duplicate this

from flask_jwt_extended.tokens import encode_access_token
from json import JSONEncoder


def make_token(username, secret_key, lifetime, claims):
    """
    Produce a JWT for access to the API.
    Parameters
    ----------
    username : str
        Name of user to issue token for.
    secret_key : str
        Secret used to sign the token.
    lifetime : timedelta
        Time for which the token is valid.
    claims : dict
        User claims. Query types this token will allow access to, and type of access allowed.
    """
    return encode_access_token(
        identity=username,
        secret=secret_key,
        algorithm="HS256",
        expires_delta=lifetime,
        fresh=True,
        user_claims=claims,
        csrf=False,
        identity_claim_key="identity",
        user_claims_key="user_claims",
        json_encoder=JSONEncoder,
    )


query_kinds = ["daily_location", "modal_location", "flows"]
permissions_types = {"run": True, "poll": True, "get_result": True}
aggregation_types = ["admin0", "admin1", "admin2", "admin3", "admin4"]
all_access_claims = {
    query_kind: {
        "permissions": permissions_types,
        "spatial_aggregation": aggregation_types,
    }
    for query_kind in query_kinds
}
