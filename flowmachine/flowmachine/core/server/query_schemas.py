import json

from hashlib import md5
from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length
from marshmallow_oneofschema import OneOfSchema


#
# Flowmachine query schemas
#


class DailyLocationSchema(Schema):
    date = fields.Date(required=True)
    method = fields.String(required=True, validate=OneOf(["last", "most-common"]))
    aggregation_unit = fields.String(
        required=True, validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    subscriber_subset = fields.String(
        required=False, allow_none=True, validate=OneOf([None])
    )

    @post_load
    def make_query_object(self, params):
        return DailyLocationExposed(**params)


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


class FlowmachineQuerySchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
    }

    def get_obj_type(self, obj):
        if isinstance(obj, DailyLocationExposed):
            return "daily_location"
        elif isinstance(obj, ModalLocationExposed):
            return "modal_location"
        else:
            raise Exception("Unknown object type: {obj.__class__.__name__}")


#
# Flowmachine query objects
#


class BaseExposedQuery:
    """
    Base class for flowmachine queries.
    """

    @property
    def query_id(self):
        return md5(json.dumps(self.query_params, sort_keys=True).encode()).hexdigest()

    @property
    def query_params(self):
        marshmallow_schema = self.__schema__()
        return marshmallow_schema.dump(self)


class DailyLocationExposed(BaseExposedQuery):

    __schema__ = DailyLocationSchema

    def __init__(self, date, *, method, aggregation_unit, subscriber_subset=None):
        self.date = date
        self.method = method
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset


class ModalLocationExposed(BaseExposedQuery):

    __schema__ = ModalLocationSchema

    def __init__(self, locations, *, aggregation_unit, subscriber_subset=None):
        self.locations = locations
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset
