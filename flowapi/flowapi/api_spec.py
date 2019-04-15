# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import yaml
from quart import Blueprint, request, jsonify, render_template, current_app
from zmq.asyncio import Socket
from flowapi import __version__

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
    flowmachine_query_schemas = reply["payload"]["query_schemas"]
    # Need to mark query_kind as a required field
    # this is a workaround because the marshmallow-oneOf plugin strips
    # the query_kind off, which means it can't be required from the marshmallow
    # side without raising an error
    for schema, schema_dict in flowmachine_query_schemas.items():
        try:
            schema_dict["required"].append("query_kind")
        except KeyError:
            pass  # Doesn't have any properties
    schema = {
        "openapi": "3.0.1",
        "info": {
            "description": "FlowKit Analytical API",
            "version": __version__,
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
                                "schema": flowmachine_query_schemas[
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
                        "500": {"description": "Server error."},
                    },
                }
            },
            "/poll/{query_id}": {
                "get": {
                    "summary": "Get the status of a query",
                    "operationId": "poll",
                    "parameters": [
                        {
                            "name": "query_id",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    ],
                    "responses": {
                        "202": {
                            "description": "Request accepted.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "status": {
                                                "type": "string",
                                                "enum": ["queued", "executing"],
                                            },
                                            "msg": {"type": "string"},
                                        },
                                    }
                                }
                            },
                        },
                        "303": {
                            "description": "Data ready.",
                            "headers": {
                                "Location": {
                                    "description": "URL to download data",
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
                        "500": {"description": "Server error."},
                        "404": {"description": "Unknown ID"},
                    },
                }
            },
            "/get/{query_id}": {
                "get": {
                    "summary": "Get the output of query",
                    "operationId": "get",
                    "parameters": [
                        {
                            "name": "query_id",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Results returning.",
                            "content": {
                                "application/json": {"schema": {"type": "object"}}
                            },
                        },
                        "202": {
                            "description": "Request accepted.",
                            "content": {
                                "application/json": {"schema": {"type": "object"}}
                            },
                        },
                        "401": {"description": "Unauthorized."},
                        "403": {
                            "description": "Bad query.",
                            "content": {
                                "application/json": {"schema": {"type": "object"}}
                            },
                        },
                        "500": {"description": "Server error."},
                        "404": {"description": "Unknown ID"},
                    },
                }
            },
            "/geography/{aggregation_unit}": {
                "get": {
                    "summary": "Get geojson for an aggregation unit",
                    "operationId": "get",
                    "parameters": [
                        {
                            "name": "aggregation_unit",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "string"},
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Downloading.",
                            "content": {
                                "application/geo+json": {"schema": {"type": "object"}}
                            },
                        },
                        "401": {"description": "Unauthorized."},
                        "500": {"description": "Server error."},
                    },
                }
            },
        },
        "components": {"schemas": flowmachine_query_schemas},
    }
    return schema


@blueprint.route("/openapi.json")
async def get_api_spec():
    return jsonify(await get_spec(request.socket, request.request_id))


@blueprint.route("/openapi.yaml")
async def get_yaml_api_spec():
    yaml_spec = yaml.dump(await get_spec(request.socket, request.request_id))
    return current_app.response_class(yaml_spec, content_type="application/x-yaml")


@blueprint.route("/openapi-redoc.json")
async def get_redoc_api_spec():
    return jsonify(
        remove_discriminators(await get_spec(request.socket, request.request_id))
    )


@blueprint.route("/redoc")
async def redoc_api_spec():
    return await render_template("spec.html", api_version=__version__)
