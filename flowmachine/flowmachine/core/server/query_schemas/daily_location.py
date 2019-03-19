from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features import daily_location
from .base_exposed_query import BaseExposedQuery

__all__ = ["DailyLocationSchema", "DailyLocationExposed"]


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


class DailyLocationExposed(BaseExposedQuery):

    __schema__ = DailyLocationSchema

    def __init__(self, date, *, method, aggregation_unit, subscriber_subset=None):
        self.date = date
        self.method = method
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset
        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        return daily_location(
            date=self.date,
            level=self.aggregation_unit,
            method=self.method,
            subscriber_subset=self.subscriber_subset,
        )
