# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import ujson as json
from quart import Blueprint, current_app, request, url_for, stream_with_context, jsonify
from .check_claims import check_claims

blueprint = Blueprint(__name__, __name__)


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
    try:
        status = message["status"]
    except KeyError:
        return jsonify({"status": "Error", "msg": "Server responded without status"}), 500
    if message["status"] == "done":
        results_streamer = stream_with_context(generate_json)(message["sql"], query_id)
        mimetype = "application/json"

        current_app.logger.debug(f"Returning result of query {query_id}.")
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
    elif status == "awol":
        return jsonify({"status": "Error", "msg": f"Route '/get/{query_id}' does not exist"}), 404
    else:
        return jsonify({"status": "Error", "msg": f"Unexpected status: {message["status"]}"}), 500


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
