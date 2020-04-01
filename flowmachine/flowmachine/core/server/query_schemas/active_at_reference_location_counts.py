# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load, Schema
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema

from flowmachine.features import ActiveAtReferenceLocationCounts
from flowmachine.features.location import RedactedActiveAtReferenceLocationCounts

from . import BaseExposedQuery
from .active_at_reference_location import ActiveAtReferenceLocationSchema

__all__ = [
    "ActiveAtReferenceLocationCountsSchema",
    "ActiveAtReferenceLocationCountsExposed",
]

from .daily_location import DailyLocationSchema
from .modal_location import ModalLocationSchema


class ReferenceLocationSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
    }


class ActiveAtReferenceLocationCountsSchema(Schema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["active_at_reference_location_counts"]))
    active_at_reference_location = ActiveAtReferenceLocationSchema()

    @post_load
    def make_query_object(self, params, **kwargs):
        return ActiveAtReferenceLocationCountsExposed(**params)


class ActiveAtReferenceLocationCountsExposed(BaseExposedQuery):
    def __init__(
        self, active_at_reference_location,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.active_at_reference_location = active_at_reference_location

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
                active_at_reference_location=self.active_at_reference_location._flowmachine_query_obj,
            )
        )
