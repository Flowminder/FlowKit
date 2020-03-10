# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from quart_jwt_extended import jwt_required, current_user
from quart import Blueprint, current_app, request, url_for, stream_with_context, jsonify
from .stream_results import stream_result_as_json

blueprint = Blueprint("query", __name__)


@blueprint.route("/run", methods=["POST"])
@jwt_required
async def run_query():
    """
    Run a query.
    ---
    post:
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/FlowmachineQuerySchema'
        required: true
      responses:
        '202':
          description: Request accepted.
          headers:
            Location:
              description: URL to poll for status
              schema:
                format: url
                type: string
        '401':
          description: Unauthorized.
        '403':
          content:
            application/json:
              schema:
                type: object
          description: Token does not grant run access to this query or spatial aggregation unit.
        '400':
          content:
            application/json:
              schema:
                type: object
          description: Query spec could not be run..
        '500':
          description: Server error.
        '503':
          description: Query is resetting; try again later.
      summary: Run a query

    """
    json_data = await request.json
    current_user.can_run(query_json=json_data)
    request.socket.send_json(
        {"request_id": request.request_id, "action": "run_query", "params": json_data}
    )

    reply = await request.socket.recv_json()
    current_app.flowapi_logger.debug(
        f"Received reply {reply}", request_id=request.request_id
    )

    if reply["status"] == "error":
        if "query_state" in reply["payload"]:
            # Query could not run due to query state
            if reply["payload"]["query_state"] == "resetting":
                return (
                    {
                        "status": "Resetting",
                        "msg": reply["msg"],
                        "payload": reply["payload"],
                    },
                    503,
                )
            else:
                return (
                    {
                        "status": "Error",
                        "msg": reply["msg"],
                        "payload": reply["payload"],
                    },
                    500,
                )
        else:
            # Query object could not be constructed
            # TODO: currently the reply msg is empty; we should either pass on the message payload (which contains
            #       further information about the error) or add a non-empty human-readable error message.
            #       If we pass on the payload we should also deconstruct it to make it more human-readable
            #       because it will contain marshmallow validation errors (and/or any other possible errors?)
            return (
                {"status": "Error", "msg": reply["msg"], "payload": reply["payload"]},
                400,
            )
    elif reply["status"] == "success":
        assert "query_id" in reply["payload"]
        d = {
            "Location": url_for(
                f"query.poll_query", query_id=reply["payload"]["query_id"]
            )
        }
        return {}, 202, d
    else:
        return (
            {"status": "Error", "msg": f"Unexpected reply status: {reply['status']}",},
            500,
        )


@blueprint.route("/poll/<query_id>")
@jwt_required
async def poll_query(query_id):
    """
    Get the status of a previously submitted query.
    ---
    get:
      parameters:
        - in: path
          name: query_id
          required: true
          schema:
            type: string
      responses:
        '202':
          content:
            application/json:
              schema:
                properties:
                  msg:
                    type: string
                  status:
                    enum:
                      - executing
                      - queued
                    type: string
                type: object
          description: Request accepted.
        '303':
          description: Data ready.
          headers:
            Location:
              description: URL to download data
              schema:
                format: url
                type: string
        '401':
          description: Unauthorized.
        '403':
          content:
            application/json:
              schema:
                type: object
          description: Token does not grant poll access to this query or spatial aggregation unit.
        '404':
          description: Unknown ID
        '500':
          description: Server error.
        '503':
          description: Query is resetting.
      summary: Get the status of a query
    """
    await current_user.can_poll_by_query_id(query_id=query_id)
    request.socket.send_json(
        {
            "request_id": request.request_id,
            "action": "poll_query",
            "params": {"query_id": query_id},
        }
    )
    reply = await request.socket.recv_json()
    current_app.flowapi_logger.debug(
        f"Received reply {reply}", request_id=request.request_id
    )

    try:
        query_state = reply["payload"]["query_state"]
    except KeyError:
        return {"status": "error", "msg": reply[""]}, 500

    response_codes = {
        "completed": 303,
        "queued": 202,
        "executing": 202,
        "unknown": 404,
        "known": 404,
        "cancelled": 404,
        "resetting": 503,
        "errored": 500,
        "reset_failed": 500,
    }

    if query_state == "completed":
        return (
            jsonify({}),
            response_codes[query_state],
            {"Location": url_for(f"query.get_query_result", query_id=query_id)},
        )
    else:
        return (
            {"status": query_state, "msg": reply["msg"]},
            response_codes.get(query_state, 500),
        )


@blueprint.route("/get/<query_id>")
@jwt_required
async def get_query_result(query_id):
    """
    Get the output of a completed query.
    ---
    get:
      parameters:
        - in: path
          name: query_id
          required: true
          schema:
            type: string
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
          description: Results returning.
        '401':
          description: Unauthorized.
        '403':
          content:
            application/json:
              schema:
                type: object
          description: Token does not grant results access to this query or spatial aggregation unit.
        '404':
          description: Result unavailable.
        '500':
          description: Server error.
      summary: Get the output of query
    """
    await current_user.can_get_results_by_query_id(query_id=query_id)
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

    if reply["status"] == "success":
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
    else:
        return {"status": "Error", "msg": reply["msg"]}, 404


@blueprint.route("/available_dates")
@jwt_required
async def get_available_dates():
    """
    Get dates available for queries.
    ---
    get:
      responses:
        '200':
          description: Dates available for each event type.
          content:
            application/json:
              schema:
                type: object
                properties:
                  calls:
                    type: array
                    items:
                      type: string
                      format: date
                  sms:
                    type: array
                    items:
                      type: string
                      format: date
                  mds:
                    type: array
                    items:
                      type: string
                      format: date
                  topups:
                    type: array
                    items:
                      type: string
                      format: date
        '401':
          description: Unauthorized.
        '403':
          content:
            application/json:
              schema:
                type: object
          description: No access with this token.
        '500':
          description: Server error.
      summary: Get the dates available to query over.
    """
    current_user.can_get_available_dates()

    request.socket.send_json(
        {"request_id": request.request_id, "action": "get_available_dates"}
    )
    reply = await request.socket.recv_json()
    current_app.flowapi_logger.debug(
        f"Received reply {reply}", request_id=request.request_id
    )

    if reply["status"] == "success":
        return {"available_dates": reply["payload"]}, 200
    else:
        assert reply["status"] == "error"
        return {"status": "error", "msg": reply["msg"]}, 500
