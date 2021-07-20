# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from quart import request
from werkzeug.exceptions import HTTPException


async def get_query_parameters_from_flowmachine(*, query_id) -> dict:
    """
    Get the parameters of a query from flowmachine.

    Parameters
    ----------
    query_id : str
        ID of the query to get params for

    Returns
    -------
    dict
        Dictionary containing the query's original parameters

    Raises
    ------
    HTTPException
        404 if the query id is not known.

    """
    request.socket.send_json(
        {
            "request_id": request.request_id,
            "action": "get_query_params",
            "params": {"query_id": query_id},
        }
    )
    reply = await request.socket.recv_json()
    if reply["status"] == "error":
        raise HTTPException(
            description=f"Unknown query ID '{query_id}'",
            name="Query ID not found",
            code=404,
        )
    return reply["payload"]["query_params"]
