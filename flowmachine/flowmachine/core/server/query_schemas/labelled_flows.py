# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates_schema, ValidationError
from marshmallow.validate import OneOf


from flowmachine.features.location.labelled_flows import LabelledFlows
from flowmachine.features.location.redacted_labelled_flows import RedactedLabelledFlows
from flowmachine.core.join import Join
from flowmachine.core.server.query_schemas.base_exposed_query import BaseExposedQuery
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.core.server.query_schemas.coalesced_location import (
    CoalescedLocationSchema,
)
from flowmachine.core.server.query_schemas.mobility_classification import (
    MobilityClassificationSchema,
)

__all__ = ["LabelledFlowsSchema", "LabelledFlowsExposed"]

from .aggregation_unit import AggregationUnitKind


class LabelledFlowsExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "labelled_flows"

    def __init__(self, *, from_location, to_location, labels, join_type):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.from_location = from_location
        self.to_location = to_location
        self.labels = labels
        self.join_type = join_type

    @property
    def aggregation_unit(self):
        return self.from_location.aggregation_unit

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
        labels = self.labels._flowmachine_query_obj
        return RedactedLabelledFlows(
            labelled_flows=LabelledFlows(
                loc1=loc1, loc2=loc2, labels=labels, join_type=self.join_type
            )
        )


# TODO: Allow InputToFlowsSchema as input, rather than just CoalescedLocationSchema


class LabelledFlowsSchema(BaseSchema):
    __model__ = LabelledFlowsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    aggregation_unit = AggregationUnitKind(dump_only=True)
    from_location = fields.Nested(CoalescedLocationSchema, required=True)
    to_location = fields.Nested(CoalescedLocationSchema, required=True)
    labels = fields.Nested(MobilityClassificationSchema, required=True)
    join_type = fields.String(validate=OneOf(Join.join_kinds), missing="inner")

    @validates_schema(skip_on_field_errors=True)
    def validate_aggregation_units(self, data, **kwargs):
        """
        Validate that from_location and to_location have the same aggregation unit
        """
        if (
            data["from_location"].aggregation_unit
            != data["to_location"].aggregation_unit
        ):
            raise ValidationError(
                "'from_location' and 'to_location' parameters must have the same aggregation unit"
            )
