# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from time import sleep

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.core.dummy_query import DummyQuery
from .aggregation_unit import AggregationUnitMixin
from .base_exposed_query import BaseExposedQuery

__all__ = ["DummyQuerySchema", "DummyQueryExposed"]

from .base_schema import BaseSchema


class DummyQueryExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "dummy_query"

    def __init__(self, dummy_param, aggregation_unit, dummy_delay):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.dummy_param = dummy_param
        self.aggregation_unit = aggregation_unit
        self.dummy_delay = dummy_delay

    @property
    def _flowmachine_query_obj(self):
        sleep(self.dummy_delay)
        return DummyQuery(dummy_param=self.dummy_param)


class DummyQuerySchema(AggregationUnitMixin, BaseSchema):
    """
    Dummy query useful for testing.
    """

    __model__ = DummyQueryExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    dummy_param = fields.String(required=True)
    dummy_delay = fields.Integer(missing=0, required=False)
