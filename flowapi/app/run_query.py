# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from functools import wraps
import ujson as json
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
            if request.path.split("/")[1] == "geography":
                query_kind = "geography"
            else:
                query_kind = (
                    "NA"
                    if json_payload is None
                    else json_payload.get("query_kind", "NA")
                )
            try:  # Get the query kind from the backend
                request.socket.send_json(
                    {
                        "request_id": request.request_id,
                        "action": "get_query_kind",
                        "query_id": kwargs["query_id"],
                    }
                )
                message = await request.socket.recv_json()
                if "query_kind" in message:
                    query_kind = message["query_kind"]
                else:
                    return jsonify({}), 404
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
                if query_kind == "geography":
                    aggregation_unit = kwargs["aggregation_unit"]
                else:
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


@blueprint.route("/run", methods=["POST"])
@check_claims("run")
async def run_query():
    json_data = await request.json
    request.socket.send_json(
        {
            "request_id": request.request_id,
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
        return jsonify({"status": "Error", "msg": message["error"]}), 403
    else:
        return jsonify({}), 403


@blueprint.route("/poll/<query_id>")
@check_claims("poll")
async def poll_query(query_id):
    request.socket.send_json(
        {"request_id": request.request_id, "action": "poll", "query_id": query_id}
    )
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
    request.socket.send_json(
        {"request_id": request.request_id, "action": "get_sql", "query_id": query_id}
    )
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
        return jsonify({"status": "Error", "msg": message["error"]}), 403
    else:
        return jsonify({}), 404


@blueprint.route("/geography/<aggregation_unit>")
@check_claims("get_result")
async def get_geography(aggregation_unit):
    request.socket.send_json(
        {
            "request_id": request.request_id,
            "action": "get_geography",
            "params": {"aggregation_unit": aggregation_unit},
        }
    )
    #  Get the reply.
    message = await request.socket.recv_json()
    current_app.logger.debug(f"Got message: {message}")
    if message["status"] == "done":
        results_streamer = stream_with_context(assemble_geojson_feature_collection)(
            message["sql"], message["crs"]
        )
        mimetype = "application/geo+json"

        current_app.logger.debug("Returning.")
        return (
            results_streamer,
            200,
            {
                "Transfer-Encoding": "chunked",
                "Content-Disposition": f"attachment;filename={aggregation_unit}.geojson",
                "Content-type": mimetype,
            },
        )
    elif message["status"] == "error":
        return jsonify({"status": "Error", "msg": message["error"]}), 403
    else:
        return jsonify({}), 404


async def assemble_geojson_feature_collection(sql_query, crs):
    """
    Assemble the GeoJSON "Feature" objects from the query response into a
    "FeatureCollection" object.

    Parameters
    ----------
    sql_query : str
        SQL query to stream output of
    crs : str
        Coordinate reference system

    Yields
    ------
    bytes
        Encoded lines of JSON

    """
    logger = current_app.logger
    pool = current_app.pool
    yield f'{{"properties":{{"crs":{crs}}}, "type":"FeatureCollection", "features":['.encode()
    prepend = ""
    logger.debug("Starting generator.")
    async with pool.acquire() as connection:
        logger.debug("Connected.")
        async with connection.transaction():
            logger.debug("Got transaction.")
            logger.debug(f"Running {sql_query}")
            try:
                async for row in connection.cursor(sql_query):
                    yield f"{prepend}{json.dumps(row[0])}".encode()
                    prepend = ", "
                logger.debug("Finishing up.")
                yield b"]}"
            except Exception as e:
                logger.error(e)


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
