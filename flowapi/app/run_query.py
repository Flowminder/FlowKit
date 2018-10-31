# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
from functools import wraps

from flask_jwt_extended import jwt_required, get_jwt_claims, get_jwt_identity
from quart import Blueprint, current_app, request, url_for, stream_with_context, jsonify

blueprint = Blueprint(__name__, __name__)


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
            json = await request.json
            query_kind = "NA" if json is None else json.get("query_kind", "NA")
            log_elements = [
                "",
                f"{func.__name__.upper()}",
                query_kind.upper(),
                get_jwt_identity(),
                request.headers.get("Remote-Addr"),
                kwargs.get("query_id", "NA"),
            ]

            current_app.query_run_logger.info(":".join(log_elements))
            try:  # Cross-check the query kind with the backend
                request.socket.send_json(
                    {"action": "get_query_kind", "query_id": kwargs["query_id"]}
                )
                message = await request.socket.recv_json()
                if "query_kind" in message:
                    query_kind = message["query_kind"]
                    log_elements[2] = query_kind.upper()
                else:
                    return jsonify({}), 404
            except KeyError:
                if query_kind == "NA":
                    return (
                        jsonify(
                            {
                                "status": "Error",
                                "reason": "Expected 'query_kind' parameter.",
                            }
                        ),
                        400,
                    )

            claims = get_jwt_claims().get(query_kind, {})
            endpoint_claims = claims.get("permissions", {})
            spatial_claims = claims.get("spatial_aggregation", {})
            if (claim_type not in endpoint_claims) or (
                endpoint_claims[claim_type] == False
            ):  # Check access claims
                log_elements[0] = "UNAUTHORIZED"
                current_app.query_run_logger.error(":".join(log_elements))
                return (
                    jsonify(
                        {
                            "status": "Error",
                            "reason": f"'{claim_type}' access denied for '{query_kind}' query",
                        }
                    ),
                    401,
                )
            elif claim_type == "get_result":  # Check spatial aggregation claims
                request.socket.send_json(
                    {"action": "get_params", "query_id": kwargs["query_id"]}
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
                                "reason": "Missing parameter: 'aggregation_unit'",
                            }
                        ),
                        500,
                    )
                if aggregation_unit not in spatial_claims:
                    log_elements[0] = "UNAUTHORIZED"
                    current_app.query_run_logger.error(":".join(log_elements))
                    return (
                        jsonify(
                            {
                                "status": "Error",
                                "reason": f"'get_result' access denied for '{aggregation_unit}' "
                                f"aggregated result of '{query_kind}' query",
                            }
                        ),
                        401,
                    )
                else:
                    pass
            else:
                pass

            return await func(*args, **kwargs)

        return wrapper

    return decorator


@blueprint.route("/run", methods=["POST"])
@check_claims("run")
async def run_query():
    json_data = await request.json
    request.socket.send_json(
        {
            "action": "run_query",
            "query_kind": json_data["query_kind"],
            "params": json_data["params"],
        }
    )

    #  Get the reply.
    message = await request.socket.recv_json()
    current_app.logger.debug(f"Received reply {message}")

    if "id" in message:
        d = {"Location": url_for(f"{__name__}.poll_query", query_id=message["id"])}
        return jsonify({}), 202, d
    elif "error" in message:
        return jsonify({"status": "Error", "reason": message["error"]}), 403
    else:
        return jsonify({}), 403


@blueprint.route("/poll/<query_id>")
@check_claims("poll")
async def poll_query(query_id):
    request.socket.send_json({"action": "poll", "query_id": query_id})
    message = await request.socket.recv_json()

    if message["status"] == "done":
        return (
            jsonify({}),
            303,
            {"Location": url_for(f"{__name__}.get_query", query_id=message["id"])},
        )
    elif message["status"] == "running":
        return jsonify({}), 202
    else:
        return jsonify({}), 404


@blueprint.route("/get/<query_id>")
@check_claims("get_result")
async def get_query(query_id):
    request.socket.send_json({"action": "get_sql", "query_id": query_id})
    message = await request.socket.recv_json()
    current_app.logger.debug(f"Got message: {message}")
    if message["status"] == "done":
        results_streamer = stream_with_context(generate_json)(message["sql"], query_id)
        mimetype = "application/json"

        current_app.logger.debug("Returning.")
        return (
            results_streamer,
            200,
            {
                "Transfer-Encoding": "chunked",
                "Content-Disposition": f"attachment;filename={query_id}.json",
                "Content-type": mimetype,
            },
        )
    elif message["status"] == "running":
        return jsonify({}), 202
    elif message["status"] == "error":
        return jsonify({"status": "Error", "reason": message["error"]}), 403
    else:
        return jsonify({}), 404


async def generate_json(sql_query, query_id):
    """
    Generate a JSON representation of a query.
    Parameters
    ----------
    sql_query : str
        SQL query to stream output of
    query_id : str
        Unique id of the query

    Yields
    ------
    bytes
        Encoded lines of JSON

    """
    logger = current_app.logger
    pool = current_app.pool
    yield f'{{"query_id":"{query_id}", "query_result":['.encode()
    prepend = ""
    logger.debug("Starting generator.")
    async with pool.acquire() as connection:
        logger.debug("Connected.")
        async with connection.transaction():
            logger.debug("Got transaction.")
            logger.debug(f"Running {sql_query}")
            try:
                async for row in connection.cursor(sql_query):
                    yield f"{prepend}{json.dumps(dict(row.items()))}".encode()
                    prepend = ", "
                logger.debug("Finishing up.")
                yield b"]}"
            except Exception as e:
                logger.error(e)
