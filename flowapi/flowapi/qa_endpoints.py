from quart_jwt_extended import jwt_required, current_user
from quart import Blueprint, current_app, request
import datetime as dt

blueprint = Blueprint("qa", __name__)


@blueprint.route("/<cdr_type>/<check_id>", methods=["GET"])
@jwt_required
async def get_qa_date_range(cdr_type, check_id):
    """
    Returns QA values for a given check type and call id between two dates
    ---
    get:
      parameters:
      - name: cdr_type
        in: path
        required: true
        schema:
          type: string
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
          description: Dates available for each event type.
          content:
            application/json:
              schema:
                type: object
                properties:
                  qa_checks:
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
      summary: Returns QA values for a given check type and call id between two dates
    """
    current_user.can_get_qa()
    return await get_qa_checks(
        cdr_type, check_id, request.args.get("start_date"), request.args.get("end_date")
    )


@blueprint.route("/<cdr_type>/<check_id>/<check_date>")
@jwt_required
async def get_qa_on_date(cdr_type, check_id, check_date):
    """
    Returns QA values for a given check type and call id on a date
    ---
    get:
      parameters:
      - name: cdr_type
        in: path
        required: true
        schema:
          type: string
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
          description: Dates available for each event type.
          content:
            application/json:
              schema:
                type: object
                properties:
                  qa_checks:
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
      summary: Returns QA values for a given cdr and check type on a given date
    """
    current_user.can_get_qa()
    return await get_qa_checks(cdr_type, check_id, check_date, check_date)


async def get_qa_checks(cdr_type, check_id, start_date, end_date):
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
        check_id=check_id,
        cdr_type=cdr_type,
    )
    current_app.query_run_logger.info("get_qa_check", params_dict)
    request.socket.send_json(
        dict(request_id=request.request_id, action="get_qa_check", params=params_dict)
    )
    reply = await request.socket.recv_json()
    if reply["status"] == "success":
        if len(reply["payload"]["qa_checks"]) == 0:
            return {
                "status": "not found",
                "msg": f"No qa checks found for {cdr_type}, {check_id} between {start_date} and {end_date}",
            }, 404
        else:
            return {"qa_checks": reply["payload"]}, 200
    else:
        assert reply["status"] == "error"
        return {"status": "error", "msg": reply["msg"]}, 500


@blueprint.route("/", methods=["GET"])
@jwt_required
async def list_qa_checks():
    """
    Lists available QA checks per CDR type for all CDR types.
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
                  available_qa_checks:
                    type: array
                    items:
                      type: string
        '401':
          description: Unauthorized.
        '500':
          description: Server error.
      summary: Get the available types of QA checks for all CDR types
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
        return {"available_qa_checks": reply["payload"]}, 200
    else:
        return {"status": "error", "msg": reply["msg"]}, 500
