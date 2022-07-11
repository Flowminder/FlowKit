# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from apispec import APISpec, yaml_utils
from quart import Blueprint, request, render_template, current_app
from zmq.asyncio import Socket
from prance.util.resolver import RefResolver
from flowapi import __version__
from flowapi.permissions import schema_to_scopes

blueprint = Blueprint("spec", __name__)


async def get_spec(socket: Socket, request_id: str) -> APISpec:
    """
    Construct open api spec by interrogating FlowMachine. Always returns the default specs of
    'run', 'get_results' and 'get_available_dates'

    Parameters
    ----------
    socket : Socket
        ZeroMQ socket used to deliver messages to Flowmachine instance
    request_id : str
        Unique id of the request

    Returns
    -------
    APISpec
        The specification object

    """
    msg = {"request_id": request_id, "action": "get_query_schemas"}
    socket.send_json(msg)
    #  Get the reply.
    reply = await socket.recv_json()
    flowmachine_query_schemas = reply["payload"]["query_schemas"]
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
    scopes = schema_to_scopes(spec.to_dict())
    breakpoint()
    r = RefResolver(spec_dict := spec.to_dict(), base_uri="")
    r.resolve_references()
    scopes = schema_to_scopes(spec_dict["components"]["schemas"])
    scopes += ["run", "get_available_dates", "get_result"]
    spec.components.security_scheme(
        "token",
        {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "x-security-scopes": sorted(scopes),
            "x-audience": current_app.config["JWT_DECODE_AUDIENCE"],
        },
    )
    # Loop over all the registered views and try to parse a yaml
    # openapi spec from their docstrings
    for rule in current_app.url_map.iter_rules():
        try:
            func = current_app.view_functions[rule.endpoint]
            operations = yaml_utils.load_operations_from_docstring(func.__doc__)
            if len(operations) > 0:
                for method, op in operations.items():
                    op["operationId"] = f"{rule.endpoint}.{method}"
                spec.path(
                    path=rule.rule,
                    operations=operations,
                )
        except Exception as e:
            pass  # Don't include in API

    return spec


@blueprint.route("/openapi.json")
async def get_api_spec():
    spec = await get_spec(request.socket, request.request_id)
    return spec.to_dict()


@blueprint.route("/openapi.yaml")
async def get_yaml_api_spec():
    spec = await get_spec(request.socket, request.request_id)
    return spec.to_yaml(), 200, dict(content_type="application/x-yaml")


@blueprint.route("/redoc")
async def redoc_api_spec():
    return await render_template("spec.html", api_version=__version__)
