from marshmallow import fields
from marshmallow.validate import OneOf, Range

from flowmachine.features.subscriber.mobility_estimation import MobilityEstimation

from .base_exposed_query import BaseExposedQuery

from .base_schema import BaseSchema
from .aggregation_unit import AggregationUnitMixin
from .field_mixins import StartAndEndField

class MobilityEstimationExposed(BaseExposedQuery):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        aggregation_unit
    ):
        self.start = start_date
        self.stop = end_date
        self.agg_unit = aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        return (
            MobilityEstimation(self.start, self.stop, self.agg_unit)
        )

class MobilityEstimationSchema(StartAndEndField, AggregationUnitMixin, BaseSchema):
    query_kind = fields.String(validate=OneOf(["mobility_estimation"]))

    __model__ = MobilityEstimationExposed