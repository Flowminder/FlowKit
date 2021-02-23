# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema


from flowmachine.features import Flows
from flowmachine.features.location.redacted_flows import RedactedFlows
from .base_exposed_query import BaseExposedQuery
from .base_schema import BaseSchema
from .daily_location import DailyLocationSchema
from .modal_location import ModalLocationSchema

__all__ = ["FlowsSchema", "FlowsExposed"]

from .most_frequent_location import MostFrequentLocationSchema

from .unique_locations import UniqueLocationsSchema


class InputToFlowsSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
        "unique_locations": UniqueLocationsSchema,
        "most_frequent_location": MostFrequentLocationSchema,
    }


class FlowsExposed(BaseExposedQuery):
    def __init__(self, *, from_location, to_location):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.from_location = from_location
        self.to_location = to_location

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
        return RedactedFlows(flows=Flows(loc1, loc2))


class FlowsSchema(BaseSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["flows"]))
    from_location = fields.Nested(InputToFlowsSchema, required=True)
    to_location = fields.Nested(InputToFlowsSchema, required=True)

    __model__ = FlowsExposed
