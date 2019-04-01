# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flask_jwt_extended import get_jwt_claims, jwt_required
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
        # TODO: currently the reply msg is empty; we should either pass on the message payload (which contains
        #       further information about the error) or add a non-empty human-readable error message.
        #       If we pass on the payload we should also deconstruct it to make it more human-readable
        #       because it will contain marshmallow validation errors (and/or any other possible errors?)
        return (
            jsonify(
                {"status": "Error", "msg": reply["msg"], "payload": reply["payload"]}
            ),
            400,
        )
    elif reply["status"] == "success":
        assert "query_id" in reply["payload"]
        d = {
            "Location": url_for(
                f"query.poll_query", query_id=reply["payload"]["query_id"]
            )
        }
        return jsonify({}), 202, d
    else:
        return (
            jsonify(
                {
                    "status": "Error",
                    "msg": f"Unexpected reply status: {reply['status']}",
                }
            ),
            500,
        )


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
        return jsonify({"status": "error", "msg": reply[""]}), 500
    else:
        assert reply["status"] == "success"
        query_state = reply["payload"]["query_state"]
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
            return jsonify({"status": query_state, "msg": reply["msg"]}), 404


@blueprint.route("/get/<query_id>")
@check_claims("get_result")
async def get_query_result(query_id):
    msg = {
        "request_id": request.request_id,
        "action": "get_sql_for_query_result",
        "params": {"query_id": query_id},
    }
    request.socket.send_json(msg)
    reply = await request.socket.recv_json()
    current_app.flowapi_logger.debug(
        f"Received reply: {reply}", request_id=request.request_id
    )

    if reply["status"] == "error":
        # TODO: check that this path is fully tested!
        query_state = reply["payload"]["query_state"]
        if query_state in ("executing", "queued"):
            return jsonify({}), 202
        elif query_state == "errored":
            return (
                jsonify({"status": "Error", "msg": reply["msg"]}),
                403,
            )  # TODO: should this really be 403?
        elif query_state in ("awol", "known"):
            return (jsonify({"status": "Error", "msg": reply["msg"]}), 404)
        else:
            return (
                jsonify(
                    {"status": "Error", "msg": f"Unexpected query state: {query_state}"}
                ),
                500,
            )
    else:
        sql = reply["payload"]["sql"]
        results_streamer = stream_with_context(stream_result_as_json)(
            sql, additional_elements={"query_id": query_id}
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


@blueprint.route("/available_dates")
@jwt_required
async def get_available_dates():
    claims = get_jwt_claims()

    def query_kind_has_any_permissions(query_kind):
        perms = claims[query_kind]["permissions"]
        return any([perms["run"], perms["poll"], perms["get_result"]])

    if not any(
        [query_kind_has_any_permissions(query_kind) for query_kind in claims.keys()]
    ):
        return jsonify({"status": "error", "msg": "Not allowed to "}), 401

    json_data = await request.json
    if json_data is None:
        event_types = None
    else:
        event_types = json_data.get("event_types", None)
    request.socket.send_json(
        {
            "request_id": request.request_id,
            "action": "get_available_dates",
            "params": {"event_types": event_types},
        }
    )
    reply = await request.socket.recv_json()

    if reply["status"] == "success":
        return jsonify({"available_dates": reply["payload"]}), 200
    else:
        assert reply["status"] == "error"
        return jsonify({"status": "error", "msg": reply["msg"]}), 500
