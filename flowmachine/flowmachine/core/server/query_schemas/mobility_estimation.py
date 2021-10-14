from marshmallow import fields
from marshmallow.validate import OneOf, Range

from flowmachine.features.subscriber.mobility_estimation import MobilityEstimation

from .base_exposed_query import BaseExposedQuery

from .base_schema import BaseSchema
from .aggregation_unit import AggregationUnitMixin
from .field_mixins import StartAndEndField


class MobilityEstimationExposed(BaseExposedQuery):
    def __init__(self, *, start_date, end_date, aggregation_unit):
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        return MobilityEstimation(self.start_date, self.end_date, self.aggregation_unit)


class MobilityEstimationSchema(StartAndEndField, AggregationUnitMixin, BaseSchema):
    query_kind = fields.String(validate=OneOf(["mobility_estimation"]))

    __model__ = MobilityEstimationExposed
