# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates_schema, ValidationError
from marshmallow.validate import OneOf

from flowmachine.features.location.unique_visitor_counts import UniqueVisitorCounts
from .active_at_reference_location_counts import ActiveAtReferenceLocationCountsSchema
from .aggregation_unit import AggregationUnitKind
from .base_schema import BaseSchema

from .unique_subscriber_counts import UniqueSubscriberCountsSchema

from . import BaseExposedQuery

__all__ = [
    "UniqueVisitorCountsSchema",
    "UniqueVisitorCountsExposed",
]


class UniqueVisitorCountsExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "unique_visitor_counts"

    def __init__(
        self,
        active_at_reference_location_counts,
        unique_subscriber_counts,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.active_at_reference_location_counts = active_at_reference_location_counts
        self.unique_subscriber_counts = unique_subscriber_counts

    @property
    def aggregation_unit(self):
        return self.unique_subscriber_counts.aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        # Note we're not using the redacted version here, because the two composing queries are redacted
        return UniqueVisitorCounts(
            active_at_reference_location_counts=self.active_at_reference_location_counts._flowmachine_query_obj,
            unique_subscriber_counts=self.unique_subscriber_counts._flowmachine_query_obj,
        )


class UniqueVisitorCountsSchema(BaseSchema):
    __model__ = UniqueVisitorCountsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    aggregation_unit = AggregationUnitKind(dump_only=True)
    active_at_reference_location_counts = fields.Nested(
        ActiveAtReferenceLocationCountsSchema(), required=True
    )
    unique_subscriber_counts = fields.Nested(
        UniqueSubscriberCountsSchema(), required=True
    )

    @validates_schema(skip_on_field_errors=True)
    def validate_aggregation_units(self, data, **kwargs):
        """
        Validate that active_at_reference_location_counts and unique_subscriber_counts have the same aggregation unit
        """
        if (
            data["active_at_reference_location_counts"].aggregation_unit
            != data["unique_subscriber_counts"].aggregation_unit
        ):
            raise ValidationError(
                "'active_at_reference_location_counts' and 'unique_subscriber_counts' parameters must have the same aggregation unit"
            )
