# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from functools import wraps
from flask_jwt_extended import jwt_required, get_jwt_claims, get_jwt_identity
from quart import current_app, request, jsonify


def check_claims(claim_type):
    """
    Create a decorator which checks the query kind provided to a route
    against the claims of any token provided.

    Parameters
    ----------
    claim_type : str
        One of "run", "poll" or "get_result"

    Returns
    -------
    decorator
    """

    def decorator(func):
        @wraps(func)
        @jwt_required
        async def wrapper(*args, **kwargs):
            json_payload = await request.json
            current_app.access_logger.info(
                "AUTHENTICATED",
                request_id=request.request_id,
                route=request.path,
                user=get_jwt_identity(),
                src_ip=request.headers.get("Remote-Addr"),
                json_payload=json_payload,
            )

            # Get query kind
            query_kind = (
                "NA" if json_payload is None else json_payload.get("query_kind", "NA")
            )
            try:  # Get the query kind from the backend
                request.socket.send_json(
                    {
                        "request_id": request.request_id,
                        "action": "get_query_kind",
                        "params": {"query_id": kwargs["query_id"]},
                    }
                )
                message = await request.socket.recv_json()
                if "query_kind" in message["data"]:
                    query_kind = message["data"]["query_kind"]
                else:
                    return jsonify({}), 404  # query does not exist
            except KeyError:
                if query_kind == "NA":
                    return (
                        jsonify(
                            {
                                "status": "Error",
                                "msg": "Expected 'query_kind' parameter.",
                            }
                        ),
                        400,
                    )

            # Get claims
            claims = get_jwt_claims().get(query_kind, {})
            endpoint_claims = claims.get("permissions", {})
            aggregation_claims = claims.get("spatial_aggregation", {})
            log_dict = dict(
                request_id=request.request_id,
                query_kind=query_kind.upper(),
                route=request.path,
                user=get_jwt_identity(),
                src_ip=request.headers.get("Remote-Addr"),
                json_payload=json_payload,
                query_id=kwargs.get("query_id", "NA"),
                claims=claims,
            )
            current_app.query_run_logger.info("Received", **log_dict)

            # Check claims
            if (claim_type not in endpoint_claims) or (
                endpoint_claims[claim_type] == False
            ):  # Check endpoint claims
                current_app.query_run_logger.error(
                    "CLAIM_TYPE_NOT_ALLOWED_BY_TOKEN", **log_dict
                )
                return (
                    jsonify(
                        {
                            "status": "Error",
                            "msg": f"'{claim_type}' access denied for '{query_kind}' query",
                        }
                    ),
                    401,
                )
            elif claim_type == "get_result":
                # Get aggregation unit
                request.socket.send_json(
                    {
                        "request_id": request.request_id,
                        "action": "get_params",
                        "query_id": kwargs["query_id"],
                    }
                )
                message = await request.socket.recv_json()
                if "params" not in message:
                    return jsonify({}), 404
                try:
                    aggregation_unit = message["params"]["aggregation_unit"]
                except KeyError:
                    return (
                        jsonify(
                            {
                                "status": "Error",
                                "msg": "Missing parameter: 'aggregation_unit'",
                            }
                        ),
                        500,
                    )
                # Check aggregation claims
                if aggregation_unit not in aggregation_claims:
                    current_app.query_run_logger.error(
                        "SPATIAL_AGGREGATION_LEVEL_NOT_ALLOWED_BY_TOKEN", **log_dict
                    )
                    return (
                        jsonify(
                            {
                                "status": "Error",
                                "msg": f"'get_result' access denied for '{aggregation_unit}' "
                                f"aggregated result of '{query_kind}' query",
                            }
                        ),
                        401,
                    )
                else:
                    pass
            else:
                pass
            current_app.query_run_logger.info("Authorised", **log_dict)
            return await func(*args, **kwargs)

        return wrapper

    return decorator


def check_geography_claims():
    """
    Create a decorator which checks the "get_result" permission for
    query kind "geography" against the claims of any token provided.

    Returns
    -------
    decorator
    """

    def decorator(func):
        @wraps(func)
        @jwt_required
        async def wrapper(*args, **kwargs):
            json_payload = await request.json
            current_app.access_logger.info(
                "AUTHENTICATED",
                request_id=request.request_id,
                route=request.path,
                user=get_jwt_identity(),
                src_ip=request.headers.get("Remote-Addr"),
                json_payload=json_payload,
            )

            query_kind = "geography"
            claim_type = "get_result"
            aggregation_unit = kwargs["aggregation_unit"]

            # Get claims
            claims = get_jwt_claims().get(query_kind, {})
            endpoint_claims = claims.get("permissions", {})
            aggregation_claims = claims.get("spatial_aggregation", {})
            log_dict = dict(
                request_id=request.request_id,
                query_kind=query_kind.upper(),
                route=request.path,
                user=get_jwt_identity(),
                src_ip=request.headers.get("Remote-Addr"),
                json_payload=json_payload,
                claims=claims,
            )
            current_app.query_run_logger.info("Received", **log_dict)

            # Check claims
            if (claim_type not in endpoint_claims) or (
                endpoint_claims[claim_type] == False
            ):  # Check endpoint claims
                current_app.query_run_logger.error(
                    "CLAIM_TYPE_NOT_ALLOWED_BY_TOKEN", **log_dict
                )
                return (
                    jsonify(
                        {
                            "status": "Error",
                            "msg": f"'{claim_type}' access denied for '{query_kind}' query",
                        }
                    ),
                    401,
                )
            # Check aggregation claims
            elif aggregation_unit not in aggregation_claims:
                current_app.query_run_logger.error(
                    "SPATIAL_AGGREGATION_LEVEL_NOT_ALLOWED_BY_TOKEN", **log_dict
                )
                return (
                    jsonify(
                        {
                            "status": "Error",
                            "msg": f"'{claim_type}' access denied for '{aggregation_unit}' "
                            f"aggregated result of '{query_kind}' query",
                        }
                    ),
                    401,
                )
            else:
                pass
            current_app.query_run_logger.info("Authorised", **log_dict)
            return await func(*args, **kwargs)

        return wrapper

    return decorator
