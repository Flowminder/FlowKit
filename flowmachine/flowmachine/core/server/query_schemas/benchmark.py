from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas import (
    BaseExposedQuery,
    FlowmachineQuerySchema,
)
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.features.benchmark.benchmark import BenchmarkQuery


# FOR NOW, this just runs the spatial aggregate schema until I can come up with
# a better way of importing all schema from flowmachine_query (which includes this schema,
# leading to a circular import)
from flowmachine.core.server.query_schemas.joined_spatial_aggregate import (
    JoinedSpatialAggregateSchema,
)
from flowmachine.core.server.query_schemas.spatial_aggregate import (
    SpatialAggregateSchema,
)
from .histogram_aggregate import HistogramAggregateSchema
from .active_at_reference_location_counts import ActiveAtReferenceLocationCountsSchema
from .consecutive_trips_od_matrix import ConsecutiveTripsODMatrixSchema
from .dummy_query import DummyQuerySchema
from .flows import FlowsSchema
from .meaningful_locations import (
    MeaningfulLocationsAggregateSchema,
    MeaningfulLocationsBetweenLabelODMatrixSchema,
    MeaningfulLocationsBetweenDatesODMatrixSchema,
)

from .aggregate_network_objects import AggregateNetworkObjectsSchema

from .geography import GeographySchema
from .location_event_counts import LocationEventCountsSchema
from .most_frequent_location import MostFrequentLocationSchema
from .trips_od_matrix import TripsODMatrixSchema
from .unique_subscriber_counts import UniqueSubscriberCountsSchema
from .location_introversion import LocationIntroversionSchema
from .total_network_objects import TotalNetworkObjectsSchema
from .dfs_metric_total_amount import DFSTotalMetricAmountSchema
from .unique_visitor_counts import UniqueVisitorCountsSchema
from .unmoving_at_reference_location_counts import (
    UnmovingAtReferenceLocationCountsSchema,
)
from .unmoving_counts import UnmovingCountsSchema

BENCHABLE_SCHEMA = [
    JoinedSpatialAggregateSchema,
    SpatialAggregateSchema,
    HistogramAggregateSchema,
    ActiveAtReferenceLocationCountsSchema,
    ConsecutiveTripsODMatrixSchema,
    DummyQuerySchema,
    FlowsSchema,
    MeaningfulLocationsAggregateSchema,
    MeaningfulLocationsBetweenLabelODMatrixSchema,
    MeaningfulLocationsBetweenDatesODMatrixSchema,
    AggregateNetworkObjectsSchema,
    GeographySchema,
    LocationEventCountsSchema,
    MostFrequentLocationSchema,
    TripsODMatrixSchema,
    UniqueSubscriberCountsSchema,
    LocationIntroversionSchema,
    TotalNetworkObjectsSchema,
    DFSTotalMetricAmountSchema,
    UniqueVisitorCountsSchema,
    UnmovingAtReferenceLocationCountsSchema,
    UnmovingCountsSchema,
]


class BenchmarkQueryExposed(BaseExposedQuery):
    def __init__(self, *, benchmark_target):
        self.benchmark_target = benchmark_target

    @property
    def _flowmachine_query_obj(self):
        benchmark_target = self.benchmark_target._flowmachine_query_obj
        return BenchmarkQuery(benchmark_target)


class BenchmarkSchema(BaseSchema):
    query_kind = fields.String(validate=OneOf(["benchmark"]))
    benchmark_target = fields.Nested(
        lambda: FlowmachineQuerySchema(exclude="benchmark")
    )
    __model__ = BenchmarkQueryExposed
