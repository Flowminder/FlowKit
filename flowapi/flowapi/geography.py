# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from quart import Blueprint, current_app, request, stream_with_context
from quart_jwt_extended import current_user, jwt_required

from .stream_results import stream_result_as_json

blueprint = Blueprint("geography", __name__)


@blueprint.route("/geography/<aggregation_unit>")
@jwt_required
async def get_geography(aggregation_unit):
    """
    Get geojson
    ---
    get:
      parameters:
        - in: path
          name: aggregation_unit
          required: true
          schema:
            type: string
      responses:
        '200':
          content:
            application/geo+json:
              schema:
                type: object
          description: Downloading.
        '401':
          description: Unauthorized.
        '403':
          content:
            application/json:
              schema:
                type: object
          description: Token does not grant run access to this spatial aggregation unit.
        '500':
          description: Server error.
      summary: Get geojson for an aggregation unit
    """
    current_user.can_get_geography(aggregation_unit=aggregation_unit)
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
        try:
            msg = reply["msg"]
        except KeyError:
            msg = "Internal server error"
        return {"status": "Error", "msg": msg}, 500

    try:
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
        #     return {"status": "Error", "msg": reply["msg"]}, 403
        # elif query_state == "awol":
        #     return ({"status": "Error", "msg": reply["msg"]}, 404)
    except KeyError:
        # TODO: This should never happen!
        return ({"status": "Error", "msg": f"No query state."}, 500)
