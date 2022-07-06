# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates_schema, ValidationError
from marshmallow.validate import OneOf

from flowmachine.features.location.active_at_reference_location_counts import (
    ActiveAtReferenceLocationCounts,
)
from flowmachine.features.subscriber.active_at_reference_location import (
    ActiveAtReferenceLocation,
)
from flowmachine.features.location.redacted_active_at_reference_location_counts import (
    RedactedActiveAtReferenceLocationCounts,
)

from . import BaseExposedQuery
from .aggregation_unit import AggregationUnitKind

__all__ = [
    "ActiveAtReferenceLocationCountsSchema",
    "ActiveAtReferenceLocationCountsExposed",
]

from .base_schema import BaseSchema

from .reference_location import ReferenceLocationSchema

from .unique_locations import UniqueLocationsSchema


class ActiveAtReferenceLocationCountsExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "active_at_reference_location_counts"

    def __init__(
        self,
        unique_locations,
        reference_locations,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.unique_locations = unique_locations
        self.reference_locations = reference_locations

    @property
    def aggregation_unit(self):
        return self.unique_locations.aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine unique locations object.

        Returns
        -------
        Query
        """
        return RedactedActiveAtReferenceLocationCounts(
            active_at_reference_location_counts=ActiveAtReferenceLocationCounts(
                active_at_reference_location=ActiveAtReferenceLocation(
                    reference_locations=self.reference_locations._flowmachine_query_obj,
                    subscriber_locations=self.unique_locations._flowmachine_query_obj,
                ),
            )
        )


class ActiveAtReferenceLocationCountsSchema(BaseSchema):
    __model__ = ActiveAtReferenceLocationCountsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    aggregation_unit = AggregationUnitKind(dump_only=True)
    unique_locations = fields.Nested(UniqueLocationsSchema(), required=True)
    reference_locations = fields.Nested(ReferenceLocationSchema(), required=True)

    @validates_schema(skip_on_field_errors=True)
    def validate_aggregation_units(self, data, **kwargs):
        """
        Validate that unique_locations and reference_locations have the same aggregation unit
        """
        if (
            data["unique_locations"].aggregation_unit
            != data["reference_locations"].aggregation_unit
        ):
            raise ValidationError(
                "'unique_locations' and 'reference_locations' parameters must have the same aggregation unit"
            )
