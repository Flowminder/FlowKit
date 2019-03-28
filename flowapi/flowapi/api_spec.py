# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from quart import (
    Blueprint,
    current_app,
    request,
    stream_with_context,
    jsonify,
    render_template,
)
from zmq.asyncio import Socket

from ._version import get_versions

blueprint = Blueprint("spec", __name__)


def remove_discriminators(spec: dict) -> dict:
    """
    Remove any discriminator keys from an openapi spec dict.

    Parameters
    ----------
    spec : dict
        Dict version of an openapi spec

    Returns
    -------
    dict
        The same spec, but with all discriminator keys removed.
    """
    newdict = {}
    for k, v in spec.items():
        if k != "discriminator":
            newdict[k] = remove_discriminators(v) if isinstance(v, dict) else v
    return newdict


async def get_spec(socket: Socket, request_id: str) -> dict:
    """
    Construct open api spec by interrogating FlowMachine.

    Parameters
    ----------
    socket : Socket
    request_id : str
        Unique id of the request

    Returns
    -------

    """
    msg = {"request_id": request_id, "action": "get_query_schemas"}
    socket.send_json(msg)
    #  Get the reply.
    reply = await socket.recv_json()
    schema = {
        "openapi": "3.0.1",
        "info": {
            "description": "FlowKit Analytical API",
            "version": get_versions()["version"],
            "title": "FlowAPI",
            "contact": {"email": "flowkit@flowminder.org"},
            "license": {
                "name": "MPLv2",
                "url": "https://www.mozilla.org/en-US/MPL/2.0/",
            },
        },
        "paths": {
            "/run_query": {
                "post": {
                    "summary": "Run a query",
                    "operationId": "runQuery",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": reply["payload"]["query_schemas"][
                                    "FlowmachineQuerySchema"
                                ]
                            }
                        },
                        "required": True,
                    },
                    "responses": {
                        "202": {
                            "description": "Request accepted.",
                            "headers": {
                                "Location": {
                                    "description": "URL to poll for status",
                                    "schema": {"type": "string", "format": "url"},
                                }
                            },
                        },
                        "401": {"description": "Unauthorized."},
                        "403": {
                            "description": "Bad query.",
                            "content": {
                                "application/json": {"schema": {"type": "object"}}
                            },
                        },
                        "404": {"description": "Unknown query type."},
                    },
                }
            }
        },
        "components": {"schemas": reply["payload"]["query_schemas"]},
    }
    return schema


@blueprint.route("/openapi.json")
async def get_api_spec():
    return jsonify(await get_spec(request.socket, request.request_id))


@blueprint.route("/openapi-redoc.json")
async def get_redoc_api_spec():
    return jsonify(
        remove_discriminators(await get_spec(request.socket, request.request_id))
    )


@blueprint.route("/redoc")
async def redoc_api_spec():
    return await render_template("spec.html", api_version=get_versions()["version"])
