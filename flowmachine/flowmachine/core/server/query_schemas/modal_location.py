# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates, ValidationError
from marshmallow.validate import OneOf, Length
from marshmallow_oneofschema import OneOfSchema

from .daily_location import DailyLocationSchema
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)


class InputToModalLocationSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {"daily_location": DailyLocationSchema}


class ModalLocationExposed(BaseExposedQueryWithSampling):
    def __init__(self, locations, *, sampling=None):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations
        self.sampling = sampling
        self.aggregation_unit = locations[0].aggregation_unit

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine ModalLocation object.

        Returns
        -------
        ModalLocation
        """
        from flowmachine.features import ModalLocation

        locations = [loc._flowmachine_query_obj for loc in self.locations]
        return ModalLocation(*locations)


class ModalLocationSchema(BaseQueryWithSamplingSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["modal_location"]))
    locations = fields.List(
        fields.Nested(InputToModalLocationSchema), validate=Length(min=1)
    )

    @validates("locations")
    def validate_locations(self, values):
        if len(set(value.aggregation_unit for value in values)) > 1:
            raise ValidationError(
                "All inputs to modal_locations should have the same aggregation unit"
            )

    __model__ = ModalLocationExposed
