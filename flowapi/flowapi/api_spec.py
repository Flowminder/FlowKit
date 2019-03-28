# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from quart import Blueprint, current_app, request, stream_with_context, jsonify

from ._version import get_versions

blueprint = Blueprint("spec", __name__)


@blueprint.route("/")
async def get_api_spec():
    msg = {"request_id": request.request_id, "action": "get_query_schemas"}
    request.socket.send_json(msg)
    #  Get the reply.
    reply = await request.socket.recv_json()
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
                                "schema": {
                                    "$ref": "#/components/schemas/FlowmachineQuerySchema"
                                }
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
    return jsonify(schema)
