# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from time import sleep

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf

from flowmachine.core.dummy_query import DummyQuery
from .aggregation_unit import AggregationUnit
from .base_exposed_query import BaseExposedQuery

__all__ = ["DummyQuerySchema", "DummyQueryExposed"]


class DummyQuerySchema(Schema):
    """
    Dummy query useful for testing.
    """

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["dummy_query"]))
    dummy_param = fields.String(required=True)
    aggregation_unit = AggregationUnit()
    dummy_delay = fields.Integer(missing=0, required=False)

    @post_load
    def make_query_object(self, params, **kwargs):
        return DummyQueryExposed(**params)


class DummyQueryExposed(BaseExposedQuery):
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
