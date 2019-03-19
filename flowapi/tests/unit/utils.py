"""
Utilities to generate access tokens for testing purposes
"""

from flask_jwt_extended.tokens import encode_access_token
from quart.json import JSONEncoder


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


query_kinds = ["daily_location", "modal_location", "flow"]
permissions_types = ["run", "poll", "get_result"]
aggregation_types = ["admin0", "admin1", "admin2", "admin3", "admin4"]
all_access_claims = {
    query_kind: {
        "permissions": permissions_types,
        "spatial_aggregation": aggregation_types,
    }
    for query_kind in query_kinds
}


exemplar_query_params = {
    "daily_location": {
        "query_kind": "daily_location",
        "date": "2016-01-01",
        "aggregation_unit": "admin3",
        "method": "last",
    },
    "modal_location": {
        "query_kind": "modal_location",
        "aggregation_unit": "admin3",
        "locations": [
            {
                "query_kind": "daily_location",
                "date": "2016-01-01",
                "aggregation_unit": "admin3",
                "method": "last",
            },
            {
                "query_kind": "daily_location",
                "date": "2016-01-02",
                "aggregation_unit": "admin3",
                "method": "last",
            },
        ],
    },
    "flow": {
        "query_kind": "flow",
        "aggregation_unit": "admin3",
        "from_location": {
            "query_kind": "daily_location",
            "date": "2016-01-01",
            "aggregation_unit": "admin3",
            "method": "last",
        },
        "to_location": {
            "query_kind": "daily_location",
            "date": "2016-01-02",
            "aggregation_unit": "admin3",
            "method": "last",
        },
    },
}
