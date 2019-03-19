from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Range

from .base_exposed_query import BaseExposedQuery


class MeaningfulLocationsSchema(Schema):
    start_date = fields.Date(required=True)
    stop_date = fields.Date(required=True)
    aggregation_unit = fields.String(
        validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    label = fields.String(required=True)
    labels = fields.Dict(
        keys=fields.String(), values=fields.Dict()
    )  # TODO: use custom field here for stricter validation!
    tower_hour_of_day_scores = fields.List(
        fields.Float(validate=Range(min=-1.0, max=1.0))
    )
    tower_day_of_week_scores = fields.Dict(
        keys=fields.String(
            validate=OneOf(
                [
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ]
            )
        ),
        values=fields.Float(validate=Range(min=-1.0, max=1.0)),
    )
    tower_cluster_radius = fields.Float()


class MeaningfulLocationsAggregateSchema(Schema):
    meaningful_locations = fields.Nested(MeaningfulLocationsSchema)
    aggregation_unit = fields.String(
        validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )

    @post_load
    def make_query_object(self, params):
        return Foobar(**params)


class MeaningfulLocationsAggregateExposed(BaseExposedQuery):

    __schema__ = MeaningfulLocationsAggregateSchema

    def __init__(self, *, meaningful_locations, aggregation_unit):
        self.meaningful_locations = meaningful_locations
        self.aggregation_unit = aggregation_unit
        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine MeaningfulLocationsAggregate object.

        Returns
        -------
        ModalLocation
        """
        from flowmachine.features import MeaningfulLocationsAggregate

        meaningful_locations = self.meaningful_locations._flowmachine_query_obj
        return MeaningfulLocationsAggregate(
            meaningful_locations=meaningful_locations, level=self.aggregation_unit
        )
