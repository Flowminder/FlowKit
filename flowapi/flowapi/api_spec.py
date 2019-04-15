# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import yaml
from apispec import APISpec, yaml_utils
from quart import Blueprint, request, jsonify, render_template, current_app
from zmq.asyncio import Socket
from flowapi import __version__

blueprint = Blueprint("spec", __name__)


def remove_discriminators(spec: dict) -> dict:
    """
    Remove any discriminator keys from an openapi spec dict, because
    they are not supported properly by redoc.

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
    spec = APISpec(
        title="FlowAPI",
        version=__version__,
        openapi_version="3.0.1",
        info=dict(
            description="FlowKit Analytical API",
            license=dict(name="MPLv2", url="https://www.mozilla.org/en-US/MPL/2.0/"),
            contact=dict(email="flowkit@flowminder.org"),
        ),
    )
    spec.components._schemas = flowmachine_query_schemas
    spec.components.security_scheme(
        "token", dict(type="http", scheme="bearer", bearerFormat="JWT")
    )
    # Loop over all the registered views and try to parse a yaml
    # openapi spec from their docstrings
    for endpoint_func_name, rule in current_app.url_map.endpoints.items():
        try:
            func = current_app.view_functions[endpoint_func_name]
            operations = yaml_utils.load_operations_from_docstring(func.__doc__)
            if len(operations) > 0:
                for method, op in operations.items():
                    op["operationId"] = f"{endpoint_func_name}.{method}"
                spec.path(
                    path=rule[
                        0
                    ].rule,  # In theory, could have multiple rules that match but will only be a single one here
                    operations=operations,
                )
        except Exception as e:
            pass  # Don't include in API

    return spec.to_dict()


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
