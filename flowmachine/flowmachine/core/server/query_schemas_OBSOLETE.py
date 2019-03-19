from copy import deepcopy
from abc import ABCMeta, abstractmethod
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length, Range
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core import Query


###############################
#                             #
#  Flowmachine query schemas  #
#                             #
###############################


class TowerDayOfWeekScore(fields.Dict):
    """
    Field that serializes to a title case string and deserializes
    to a lower case string.
    """

    def _deserialize(self, value, attr, data, **kwargs):
        ttt = super()._deserialize(value, attr, data, **kwargs)
        breakpoint()
        pass

    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return ""
        return value.title()

    def _deserialize(self, value, attr, data, **kwargs):
        return value.lower()


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


####################################################
#                                                  #
#  Flowmachine exposed query objects               #
#                                                  #
#  These are constructed using the schemas above.  #
#                                                  #
####################################################


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
