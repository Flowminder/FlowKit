# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema

from flowmachine.features.subscriber.active_at_reference_location import (
    ActiveAtReferenceLocation,
)
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["ActiveAtReferenceLocationSchema", "ActiveAtReferenceLocationExposed"]

from .daily_location import DailyLocationSchema
from .modal_location import ModalLocationSchema
from .unique_locations import UniqueLocationsSchema


class ReferenceLocationSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
    }


class ActiveAtReferenceLocationSchema(BaseQueryWithSamplingSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["active_at_reference_location"]))
    unique_locations = UniqueLocationsSchema()
    reference_locations = ReferenceLocationSchema()

    @post_load
    def make_query_object(self, params, **kwargs):
        return ActiveAtReferenceLocationExposed(**params)


class ActiveAtReferenceLocationExposed(BaseExposedQueryWithSampling):
    def __init__(self, unique_locations, reference_locations, sampling=None):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.reference_locations = reference_locations
        self.unique_locations = unique_locations
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine unique locations object.

        Returns
        -------
        Query
        """
        return ActiveAtReferenceLocation(
            reference_locations=self.reference_locations._flowmachine_query_obj,
            subscriber_locations=self.unique_locations._flowmachine_query_obj,
        )
