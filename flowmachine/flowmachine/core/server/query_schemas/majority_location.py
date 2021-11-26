# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from marshmallow import fields
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core.server.query_schemas import BaseExposedQuery
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.core.server.query_schemas.location_visits import LocationVisitsSchema
from flowmachine.features.subscriber.majority_location import MajorityLocation


class MajorityLocationExposed(BaseExposedQuery):
    def __init__(self, *, subscriber_location_weights, include_unlocatable):
        self.subscriber_location_weights = subscriber_location_weights
        self.include_unlocatable = include_unlocatable

    @property
    def _flowmachine_query_obj(self):
        return MajorityLocation(
            subscriber_location_weights=self.subscriber_location_weights,
            weight_column="value",
            include_unlocatable=self.include_unlocatable,
        )


# Blocked till LocationVisits is exposed
class LocatableQueries(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {"location_visits": LocationVisitsSchema}


class MajorityLocationSchema(BaseSchema):
    query_kind = fields.String(validate=OneOf(["majority_location"]))
    subscriber_location_weights = fields.Nested(LocatableQueries)
    include_unlocatable = fields.Boolean(missing=False)
