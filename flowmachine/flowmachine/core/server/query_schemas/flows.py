# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf


from flowmachine.features import Flows
from flowmachine.features.location.redacted_flows import (
    RedactedFlows,
)
from flowmachine.core.join import Join
from flowmachine.core.server.query_schemas.base_exposed_query import BaseExposedQuery
from flowmachine.core.server.query_schemas.base_schema import BaseSchema

__all__ = ["FlowsSchema", "FlowsExposed"]

from .reference_location import ReferenceLocationSchema
from .one_of_query import OneOfQuerySchema

from .unique_locations import UniqueLocationsSchema


class InputToFlowsSchema(OneOfQuerySchema):
    query_schemas = (
        *ReferenceLocationSchema.query_schemas,
        UniqueLocationsSchema,
    )


class FlowsExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "flows"

    def __init__(self, *, from_location, to_location, join_type):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.from_location = from_location
        self.to_location = to_location
        self.join_type = join_type

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        loc1 = self.from_location._flowmachine_query_obj
        loc2 = self.to_location._flowmachine_query_obj
        return RedactedFlows(flows=Flows(loc1, loc2, join_type=self.join_type))


class FlowsSchema(BaseSchema):
    __model__ = FlowsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    from_location = fields.Nested(InputToFlowsSchema, required=True)
    to_location = fields.Nested(InputToFlowsSchema, required=True)
    join_type = fields.String(validate=OneOf(Join.join_kinds), missing="inner")
