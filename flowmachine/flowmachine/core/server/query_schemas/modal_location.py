# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates, ValidationError
from marshmallow.validate import OneOf, Length

from .daily_location import DailyLocationSchema
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .one_of_query import OneOfQuerySchema


class InputToModalLocationSchema(OneOfQuerySchema):
    query_schemas = (DailyLocationSchema,)


class ModalLocationExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "modal_location"

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
    __model__ = ModalLocationExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    locations = fields.List(
        fields.Nested(InputToModalLocationSchema), validate=Length(min=1), required=True
    )

    @validates("locations")
    def validate_locations(self, values):
        # Filtering out locations with no aggregation_unit attribute -
        # validation errors for these should be raised when validating the
        # individual location query specs
        if (
            len(
                set(
                    value.aggregation_unit
                    for value in values
                    if hasattr(value, "aggregation_unit")
                )
            )
            > 1
        ):
            raise ValidationError(
                "All inputs to modal_location should have the same aggregation unit"
            )
