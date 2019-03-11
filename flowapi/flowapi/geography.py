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
    request.socket.send_json(
        {
            "request_id": request.request_id,
            "action": "get_geography",
            "params": {"aggregation_unit": aggregation_unit},
        }
    )
    #  Get the reply.
    message = await request.socket.recv_json()
    current_app.log.debug(f"Got message: {message}", request_id=request.request_id)
    try:
        status = message["status"]
    except KeyError:
        return (
            jsonify({"status": "Error", "msg": "Server responded without status"}),
            500,
        )
    if status == "done":
        results_streamer = stream_with_context(stream_result_as_json)(
            message["sql"],
            result_name="features",
            additional_elements={"type": "FeatureCollection"},
        )
        mimetype = "application/geo+json"

        current_app.log.debug(
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
    elif status == "error":
        return jsonify({"status": "Error", "msg": message["error"]}), 403
    elif status == "awol":
        return (jsonify({"status": "Error", "msg": message["error"]}), 404)
    else:
        return (
            jsonify({"status": "Error", "msg": f"Unexpected status: {status}"}),
            500,
        )
