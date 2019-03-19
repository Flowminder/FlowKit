from marshmallow import Schema, fields, post_load

from flowmachine.core.dummy_query import DummyQuery
from .base_exposed_query import BaseExposedQuery

__all__ = ["DummyQuerySchema", "DummyQueryExposed"]


class DummyQuerySchema(Schema):
    """
    Dummy query useful for testing.
    """

    dummy_param = fields.String(required=True)

    @post_load
    def make_query_object(self, params):
        return DummyQueryExposed(**params)


class DummyQueryExposed(BaseExposedQuery):

    __schema__ = DummyQuerySchema

    def __init__(self, dummy_param):
        self.dummy_param = dummy_param
        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        return DummyQuery(dummy_param=self.dummy_param)
