# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema


from flowmachine.features import Flows
from .base_exposed_query import BaseExposedQuery
from .daily_location import DailyLocationSchema, DailyLocationExposed
from .modal_location import ModalLocationSchema, ModalLocationExposed
from .custom_fields import AggregationUnit

__all__ = ["FlowsSchema", "FlowsExposed"]


class InputToFlowsSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
    }


class FlowsSchema(Schema):
    query_kind = fields.String(validate=OneOf(["flows"]))
    from_location = fields.Nested(InputToFlowsSchema, required=True)
    to_location = fields.Nested(InputToFlowsSchema, required=True)
    # TODO: validate that the aggregation unit coincides with the ones in {from|to}_location
    aggregation_unit = AggregationUnit(required=True)

    @post_load
    def make_query_object(self, params):
        return FlowsExposed(**params)


class FlowsExposed(BaseExposedQuery):
    def __init__(self, *, from_location, to_location, aggregation_unit):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.from_location = from_location
        self.to_location = to_location
        self.aggregation_unit = aggregation_unit

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
        return Flows(loc1, loc2)
