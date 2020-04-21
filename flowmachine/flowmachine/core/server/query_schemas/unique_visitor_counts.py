# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features.location.unique_visitor_counts import UniqueVisitorCounts
from .active_at_reference_location_counts import ActiveAtReferenceLocationCountsSchema
from .base_schema import BaseSchema

from .unique_subscriber_counts import UniqueSubscriberCountsSchema

from . import BaseExposedQuery

__all__ = [
    "UniqueVisitorCountsSchema",
    "UniqueVisitorCountsExposed",
]


class UniqueVisitorCountsExposed(BaseExposedQuery):
    def __init__(
        self, active_at_reference_location_counts, unique_subscriber_counts,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.active_at_reference_location_counts = active_at_reference_location_counts
        self.unique_subscriber_counts = unique_subscriber_counts

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
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["unique_visitor_counts"]))
    active_at_reference_location_counts = fields.Nested(
        ActiveAtReferenceLocationCountsSchema()
    )
    unique_subscriber_counts = fields.Nested(UniqueSubscriberCountsSchema())

    __model__ = UniqueVisitorCountsExposed
