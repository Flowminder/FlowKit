# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from functools import wraps
from flask_jwt_extended import (
    jwt_required,
    get_jwt_claims,
    get_jwt_identity,
    current_user,
)
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
    if claim_type not in {"run", "poll", "get_result"}:
        raise ValueError(f'{claim_type} not one of "run", "poll" or "get_result"')

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

            # TODO: make the claim type an enum!
            # TODO: Review whether this shouldn't be split up into multiple funcs
            if claim_type == "run":
                try:
                    query_kind = json_payload["query_kind"]
                except KeyError:
                    error_msg = "Query kind must be specified when running a query."
                    return jsonify({"msg": error_msg}), 400
            else:  # elif claim_type in ["poll", "get_result"]:
                # Ask flowmachine server for the query kind of the given query_id
                query_id = kwargs["query_id"]

                msg = {
                    "request_id": request.request_id,
                    "action": "get_query_params",
                    "params": {"query_id": query_id},
                }
                request.socket.send_json(msg)
                reply = await request.socket.recv_json()
                if reply["status"] == "error":
                    # TODO: ensure/verify that the error means the query doesn't exist
                    #
                    # We return 401 here instead of 404 because we don't want an unauthorised
                    # user to be able to infer which queries do or don't exist.
                    return jsonify({}), 401
                query_kind = reply["payload"]["query_kind"]
                aggregation_unit = reply["payload"]["aggregation_unit"]

            log_dict = dict(
                request_id=request.request_id,
                query_kind=query_kind.upper(),
                route=request.path,
                user=current_user.__dict__,
                src_ip=request.headers.get("Remote-Addr"),
                json_payload=json_payload,
                query_id=kwargs.get("query_id", "NA"),
            )
            current_app.query_run_logger.info("Received", **log_dict)
            current_user.has_access(
                claim_type=claim_type,
                query_kind=query_kind,
                aggregation_unit=aggregation_unit,
            )
            current_app.query_run_logger.info("Authorised", **log_dict)
            return await func(*args, **kwargs)

        return wrapper

    return decorator
