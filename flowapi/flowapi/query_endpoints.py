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
        {"request_id": request.request_id, "action": "run_query", "params": json_data}
    )

    reply = await request.socket.recv_json()
    current_app.flowapi_logger.debug(
        f"Received reply {reply}", request_id=request.request_id
    )

    if reply["status"] == "error":
        return jsonify({"status": "Error", "msg": reply["msg"]}), 403
    elif reply["status"] == "accepted":
        assert "query_id" in reply["data"]
        d = {
            "Location": url_for(f"query.poll_query", query_id=reply["data"]["query_id"])
        }
        return jsonify({}), 202, d
    else:
        return jsonify({}), 403


@blueprint.route("/poll/<query_id>")
@check_claims("poll")
async def poll_query(query_id):
    request.socket.send_json(
        {
            "request_id": request.request_id,
            "action": "poll_query",
            "params": {"query_id": query_id},
        }
    )
    reply = await request.socket.recv_json()

    if reply["status"] == "error":
        return jsonify({"status": "error", "msg": reply[""]})
    else:
        assert reply["status"] == "done"
        query_state = reply["data"]["query_state"]
        if query_state == "completed":
            return (
                jsonify({}),
                303,
                {"Location": url_for(f"query.get_query_result", query_id=query_id)},
            )
        elif query_state in ("executing", "queued"):
            return jsonify({"status": query_state, "msg": reply["msg"]}), 202
        elif query_state in ("errored", "cancelled"):
            return jsonify({"status": query_state, "msg": reply["msg"]}), 500
        else:  # TODO: would be good to have an explicit query state for this, too!
            # breakpoint()
            return jsonify({"status": query_state, "msg": reply["msg"]}), 404


@blueprint.route("/get/<query_id>")
@check_claims("get_result")
async def get_query_result(query_id):
    request.socket.send_json(
        {
            "request_id": request.request_id,
            "action": "get_sql_for_query_result",
            "query_id": query_id,
        }
    )
    message = await request.socket.recv_json()
    current_app.flowapi_logger.debug(
        f"Got message: {message}", request_id=request.request_id
    )
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

        current_app.flowapi_logger.debug(
            f"Returning result of query {query_id}.", request_id=request.request_id
        )
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
