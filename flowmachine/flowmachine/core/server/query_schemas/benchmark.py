from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas import BaseExposedQuery
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.features.benchmark.benchmark import BenchmarkQuery


# FOR NOW, this just runs the spatial aggregate schema until I can come up with
# a better way of importing all schema from flowmachine_query (which includes this schema,
# leading to a circular import)
from flowmachine.core.server.query_schemas.spatial_aggregate import (
    SpatialAggregateSchema,
)

class BenchmarkQueryExposed(BaseExposedQuery):
    def __init__(self, *, benchmark_target):
        self.benchmark_target = benchmark_target

    @property
    def _flowmachine_query_obj(self):
        benchmark_target = self.benchmark_target._flowmachine_query_obj
        return BenchmarkQuery(benchmark_target)


class BenchmarkSchema(BaseSchema):
    query_kind = fields.String(validate=OneOf(["benchmark"]))
    benchmark_target = fields.Nested(SpatialAggregateSchema)
    __model__ = BenchmarkQueryExposed
