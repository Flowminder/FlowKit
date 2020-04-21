# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features.location.redacted_unmoving_at_reference_location_counts import (
    RedactedUnmovingAtReferenceLocationCounts,
)
from flowmachine.features.location.unmoving_at_reference_location_counts import (
    UnmovingAtReferenceLocationCounts,
)
from flowmachine.features.subscriber.unmoving_at_reference_location import (
    UnmovingAtReferenceLocation,
)
from . import BaseExposedQuery
from .base_schema import BaseSchema
from .reference_location import ReferenceLocationSchema
from .unique_locations import UniqueLocationsSchema

__all__ = [
    "UnmovingAtReferenceLocationCountsSchema",
    "UnmovingAtReferenceLocationCountsExposed",
]


class UnmovingAtReferenceLocationCountsExposed(BaseExposedQuery):
    def __init__(self, locations, reference_locations):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations
        self.reference_locations = reference_locations

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        return RedactedUnmovingAtReferenceLocationCounts(
            unmoving_at_reference_location_counts=UnmovingAtReferenceLocationCounts(
                UnmovingAtReferenceLocation(
                    locations=self.locations._flowmachine_query_obj,
                    reference_locations=self.reference_locations._flowmachine_query_obj,
                )
            )
        )


class UnmovingAtReferenceLocationCountsSchema(BaseSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(
        validate=OneOf(["unmoving_at_reference_location_counts"])
    )
    locations = fields.Nested(UniqueLocationsSchema)
    reference_locations = fields.Nested(ReferenceLocationSchema)

    __model__ = UnmovingAtReferenceLocationCountsExposed
