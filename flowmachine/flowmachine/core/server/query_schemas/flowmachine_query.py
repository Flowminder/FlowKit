# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from functools import lru_cache

from apispec import APISpec
from apispec_oneofschema import MarshmallowPlugin

from marshmallow_oneofschema import OneOfSchema

from .util import get_type_schemas_from_entrypoint


class FlowmachineQuerySchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = get_type_schemas_from_entrypoint("top_level_queries")


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
    spec.components.schema("FlowmachineQuerySchema", schema=FlowmachineQuerySchema)
    return spec.to_dict()["components"]["schemas"]
