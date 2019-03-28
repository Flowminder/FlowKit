# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from quart import Blueprint, current_app, request, stream_with_context, jsonify
from .stream_results import stream_result_as_json
from .check_claims import check_geography_claims

blueprint = Blueprint("geography", __name__)


@blueprint.route("/geography/<aggregation_unit>")
@check_geography_claims()
async def get_geography(aggregation_unit):
    msg = {
        "request_id": request.request_id,
        "action": "get_geography",
        "params": {"aggregation_unit": aggregation_unit},
    }
    request.socket.send_json(msg)
    #  Get the reply.
    reply = await request.socket.recv_json()
    current_app.flowapi_logger.debug(
        f"Got message: {reply}", request_id=request.request_id
    )

    if reply["status"] == "error":
        return jsonify({"status": "Error", "msg": "Internal server error"}), 500

    query_state = reply["payload"]["query_state"]
    if query_state == "completed":
        results_streamer = stream_with_context(stream_result_as_json)(
            reply["payload"]["sql"],
            result_name="features",
            additional_elements={"type": "FeatureCollection"},
        )
        mimetype = "application/geo+json"

        current_app.flowapi_logger.debug(
            f"Returning {aggregation_unit} geography data.",
            request_id=request.request_id,
        )
        return (
            results_streamer,
            200,
            {
                "Transfer-Encoding": "chunked",
                "Content-Disposition": f"attachment;filename={aggregation_unit}.geojson",
                "Content-type": mimetype,
            },
        )
    # TODO: Reinstate correct status codes for geographies
    #
    # elif query_state == "error":
    #     return jsonify({"status": "Error", "msg": reply["msg"]}), 403
    # elif query_state == "awol":
    #     return (jsonify({"status": "Error", "msg": reply["msg"]}), 404)
    else:
        return (
            jsonify(
                {"status": "Error", "msg": f"Unexpected query state: {query_state}"}
            ),
            500,
        )
