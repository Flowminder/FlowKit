# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from functools import lru_cache

import pkg_resources
from apispec import APISpec
from apispec_oneofschema import MarshmallowPlugin

from marshmallow_oneofschema import OneOfSchema


def get_schema() -> OneOfSchema:
    discovered_plugins = {
        entry_point.name: entry_point.load()
        for entry_point in pkg_resources.iter_entry_points("flowkit.queries")
    }

    class FlowmachineQuerySchema(OneOfSchema):
        type_field = "query_kind"
        type_schemas = {k: v for d in discovered_plugins.values() for k, v in d.items()}

    return FlowmachineQuerySchema


@lru_cache(maxsize=1)
def get_query_schema() -> dict:
    """
    Get a dictionary representation of the FlowmachineQuerySchema api spec.
    This will contain a schema which defines all valid query types that can be run.

    Returns
    -------
    dict

    """
    spec = APISpec(
        title="FlowAPI",
        version="1.0.0",
        openapi_version="3.0.2",
        plugins=[MarshmallowPlugin()],
    )
    spec.components.schema("FlowmachineQuerySchema", schema=get_schema())
    return spec.to_dict()["components"]["schemas"]
