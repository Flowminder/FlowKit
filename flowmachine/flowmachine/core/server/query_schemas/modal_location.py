from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length
from marshmallow_oneofschema import OneOfSchema

from .base_exposed_query import BaseExposedQuery
from .daily_location import DailyLocationSchema, DailyLocationExposed


class InputToModalLocationSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {"daily_location": DailyLocationSchema}

    def get_obj_type(self, obj):
        if isinstance(obj, DailyLocationExposed):
            return "daily_location"
        else:
            raise Exception("Unknown object type: {obj.__class__.__name__}")


class ModalLocationSchema(Schema):
    locations = fields.Nested(
        InputToModalLocationSchema, many=True, validate=Length(min=1)
    )
    aggregation_unit = fields.String(
        validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    subscriber_subset = fields.String(allow_none=True, validate=OneOf([None]))

    @post_load
    def make_query_object(self, data):
        return ModalLocationExposed(**data)


class ModalLocationExposed(BaseExposedQuery):

    __schema__ = ModalLocationSchema

    def __init__(self, locations, *, aggregation_unit, subscriber_subset=None):
        self.locations = locations
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset
        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine ModalLocation object.

        Returns
        -------
        ModalLocation
        """
        from flowmachine.features import ModalLocation

        locations = [loc._flowmachine_query_obj for loc in self.locations]
        return ModalLocation(*locations)
