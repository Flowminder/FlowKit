# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from marshmallow import (
    fields,
    ValidationError,
    validates_schema,
)
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema


from flowmachine.features.location.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)
from flowmachine.features.location.redacted_joined_spatial_aggregate import (
    RedactedJoinedSpatialAggregate,
)
from .base_exposed_query import BaseExposedQuery
from .base_schema import BaseSchema
from .reference_location import ReferenceLocationSchema
from .util import get_type_schemas_from_entrypoint


__all__ = ["JoinedSpatialAggregateSchema", "JoinedSpatialAggregateExposed"]


class JoinableMetrics(OneOfSchema):
    type_field = "query_kind"
    type_schemas = get_type_schemas_from_entrypoint("joinable_queries")


class JoinedSpatialAggregateExposed(BaseExposedQuery):
    def __init__(self, *, locations, metric, method):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations
        self.metric = metric
        self.method = method

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        locations = self.locations._flowmachine_query_obj
        metric = self.metric._flowmachine_query_obj
        return RedactedJoinedSpatialAggregate(
            joined_spatial_aggregate=JoinedSpatialAggregate(
                locations=locations, metric=metric, method=self.method
            )
        )


class JoinedSpatialAggregateSchema(BaseSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["joined_spatial_aggregate"]))
    locations = fields.Nested(ReferenceLocationSchema, required=True)
    metric = fields.Nested(JoinableMetrics, required=True)
    method = fields.String(validate=OneOf(JoinedSpatialAggregate.allowed_methods))

    @validates_schema(pass_original=True)
    def validate_method(self, data, original_data, **kwargs):
        try:
            OneOf(
                self._declared_fields["metric"]
                .schema.type_schemas[original_data["metric"]["query_kind"]]
                .valid_metrics
            )(original_data["method"])
        except AttributeError:
            raise ValidationError(
                f"{original_data['metric']['query_kind']} does not have a valid metric type."
            )
        except ValidationError as exc:
            raise ValidationError(exc.messages, "method")

    __model__ = JoinedSpatialAggregateExposed
