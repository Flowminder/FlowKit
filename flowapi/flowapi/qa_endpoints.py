from quart_jwt_extended import jwt_required, current_user
from quart import Blueprint, current_app, request
from typing import Tuple, List, Union
import datetime as dt

blueprint = Blueprint("qa", __name__)


@blueprint.route("/<cdr_type>/<check_id>", methods=["GET"])
@jwt_required
async def get_qa_date_range(cdr_type, check_id):
    """
    Returns QA values for a given check on a datatype between two dates
    ---
    get:
      parameters:
      - name: cdr_type
        in: path
        required: true
        schema:
          type: string
          enum:
          - calls
          - sms
          - mds
          - topups
          - forwards
          - cell_info
      - name: check_id
        in: path
        required: true
        schema:
          type: string
      - name: start_date
        required: true
        schema:
          type: string
          format: date
        in: query
      - name: end_date
        required: true
        schema:
          type: string
          format: date
        in: query
      responses:
        '200':
          description: QA outcomes.
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    outcome:
                      type: string
                    type_of_query_or_check:
                      type: string
                    cdr_date:
                      type: string
                      format: date
        '400':
          description: Bad request
        '401':
          description: Unauthorized.
        '404':
          description: No QA checks of type specified found on date
        '500':
          description: Server error.
      summary: Get QA outcomes for a given check on a datatype between two dates
    """
    current_user.can_get_qa()
    return await get_qa_checks(
        cdr_type, check_id, request.args.get("start_date"), request.args.get("end_date")
    )


@blueprint.route("/<cdr_type>/<check_id>/<check_date>")
@jwt_required
async def get_qa_on_date(cdr_type, check_id, check_date):
    """
    Returns QA values for a given check on a date of a datatype.
    ---
    get:
      parameters:
      - name: cdr_type
        in: path
        required: true
        schema:
          type: string
          enum:
          - calls
          - sms
          - mds
          - topups
          - forwards
          - cell_info
      - name: check_id
        in: path
        required: true
        schema:
          type: string
      - name: check_date
        in: path
        required: true
        schema:
          type: string
          format: date
      responses:
        '200':
          description: QA check outcome.
          content:
            text/html:
              schema:
                type: string
        '400':
          description: Bad request
        '401':
          description: Unauthorized.
        '404':
          description: No QA checks of type specified found on date
        '500':
          description: Server error.
      summary: Get QA check outcome for a check on one date of a datatype.
    """
    current_user.can_get_qa()
    reply, code = await get_qa_checks(cdr_type, check_id, check_date, check_date)
    if code == 404:
        reply["msg"] = f"No qa checks found for {cdr_type}, {check_id} on {check_date}"
    elif code == 200:
        reply = reply[0]["outcome"]
    return reply, code


async def get_qa_checks(
    cdr_type: str, check_id: str, start_date: str, end_date: str
) -> Tuple[Union[dict, List[dict]], int]:
    """
    Validates start_date and end_date are date formatted strings, and then requests the list of QA
    checks via zeromq from flowmachine.

    Parameters
    ----------
    cdr_type : str
      The event type to get checks on
    check_id : str
      The specific check to get outcomes of
    start_date, end_date : str
      YYYY-MM-dd format strings for interval to get checks over (end_date inclusive)

    Returns
    -------
    tuple of dict or list and int
      A tuple with either of list of qa outcome dicts, or an error message dict, with the
      appropriate HTTP status code in either case.
    """
    try:
        dt.datetime.strptime(start_date, "%Y-%m-%d")
        dt.datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return (
            dict(
                status="bad request",
                msg=f"Could not parse {start_date} or {end_date}; expected for format is YYYY-MM-DD",
            ),
            400,
        )
    params_dict = dict(
        start_date=start_date,
        end_date=end_date,
        check_type=check_id,
        cdr_type=cdr_type,
    )
    current_app.query_run_logger.info("get_qa_checks", params_dict)
    request.socket.send_json(
        dict(request_id=request.request_id, action="get_qa_checks", params=params_dict)
    )
    reply = await request.socket.recv_json()
    if reply["status"] == "success":
        if len(reply["payload"]["qa_checks"]) == 0:
            return {
                "status": "not found",
                "msg": f"No qa checks found for {cdr_type}, {check_id} between {start_date} and {end_date}",
            }, 404
        else:
            return reply["payload"]["qa_checks"], 200
    else:
        return {"status": "error", "msg": reply["msg"]}, 500


@blueprint.route("/", methods=["GET"])
@jwt_required
async def list_qa_checks():
    """
    Lists available QA checks per datatype for all datatypes.
    ---
    get:
      responses:
        '200':
          description: Types of QA checks for all datatypes.
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    cdr_type:
                        enum:
                          - calls
                          - sms
                          - mds
                          - topups
                          - forwards
                          - cell_info
                    type_of_query_or_check:
                      type: string
        '401':
          description: Unauthorized.
        '500':
          description: Server error.
      summary: Returns the available types of QA checks for all datatypes
    """
    current_user.can_get_qa()
    current_app.query_run_logger.info("list_qa_checks")
    request.socket.send_json(
        dict(
            request_id=request.request_id,
            action="list_qa_checks",
        )
    )
    reply = await request.socket.recv_json()
    if reply["status"] == "success":
        return reply["payload"]["available_qa_checks"], 200
    else:
        return {"status": "error", "msg": reply["msg"]}, 500
