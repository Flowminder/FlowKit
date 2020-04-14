# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features.location.redacted_unmoving_counts import (
    RedactedUnmovingCounts,
)
from flowmachine.features.location.unmoving_counts import UnmovingCounts
from flowmachine.features.subscriber.unmoving import Unmoving
from . import BaseExposedQuery
from .base_schema import BaseSchema
from .unique_locations import UniqueLocationsSchema

__all__ = ["UnmovingCountsSchema", "UnmovingCountsExposed"]


class UnmovingCountsExposed(BaseExposedQuery):
    def __init__(self, locations):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        return RedactedUnmovingCounts(
            unmoving_counts=UnmovingCounts(
                Unmoving(self.locations._flowmachine_query_obj)
            )
        )


class UnmovingCountsSchema(BaseSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["unmoving_counts"]))
    locations = fields.Nested(UniqueLocationsSchema)

    __model__ = UnmovingCountsExposed
