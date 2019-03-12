# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from quart import Blueprint, current_app, request, url_for, stream_with_context, jsonify
from .stream_results import stream_result_as_json
from .check_claims import check_claims

blueprint = Blueprint("query", __name__)


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
        d = {"Location": url_for(f"query.poll_query", query_id=message["id"])}
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

    if message["status"] == "completed":
        return (
            jsonify({}),
            303,
            {"Location": url_for(f"query.get_query", query_id=message["id"])},
        )
    elif message["status"] in ("executing", "queued"):
        return jsonify({"status": message["status"]}), 202
    elif message["status"] in ("errored", "cancelled"):
        return jsonify({"status": message["status"]}), 500
    else:
        return jsonify({"status": message["status"]}), 404


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
        return (
            jsonify({"status": "Error", "msg": "Server responded without status"}),
            500,
        )
    if message["status"] == "completed":
        results_streamer = stream_with_context(stream_result_as_json)(
            message["sql"], additional_elements={"query_id": query_id}
        )
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
    elif message["status"] in ("executing", "queued"):
        return jsonify({}), 202
    elif message["status"] == "errored":
        return jsonify({"status": "Error", "msg": message["error"]}), 403
    elif status in ("awol", "known"):
        return (jsonify({"status": "Error", "msg": message["error"]}), 404)
    else:
        return jsonify({"status": "Error", "msg": f"Unexpected status: {status}"}), 500
