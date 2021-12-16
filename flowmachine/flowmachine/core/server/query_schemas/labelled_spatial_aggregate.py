# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features.location.redacted_labelled_spatial_aggregate import (
    RedactedLabelledSpatialAggregate,
)
from flowmachine.features.location.labelled_spatial_aggregate import (
    LabelledSpatialAggregate,
)
from flowmachine.core.server.query_schemas.base_exposed_query import BaseExposedQuery
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.core.server.query_schemas.coalesced_location import (
    CoalescedLocationSchema,
)
from flowmachine.core.server.query_schemas.mobility_classification import (
    MobilityClassificationSchema,
)

__all__ = [
    "LabelledSpatialAggregateSchema",
    "LabelledSpatialAggregateExposed",
]


class LabelledSpatialAggregateExposed(BaseExposedQuery):
    def __init__(self, *, locations, labels):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations
        self.labels = labels

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        return RedactedLabelledSpatialAggregate(
            labelled_spatial_aggregate=LabelledSpatialAggregate(
                locations=self.locations._flowmachine_query_obj,
                labels=self.labels._flowmachine_query_obj,
            )
        )


class LabelledSpatialAggregateSchema(BaseSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["labelled_spatial_aggregate"]))
    locations = fields.Nested(CoalescedLocationSchema, required=True)
    labels = fields.Nested(MobilityClassificationSchema, required=True)

    __model__ = LabelledSpatialAggregateExposed
