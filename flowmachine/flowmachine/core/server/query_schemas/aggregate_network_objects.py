# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import AggregateNetworkObjects
from .base_exposed_query import BaseExposedQuery
from .base_schema import BaseSchema
from .total_network_objects import TotalNetworkObjectsSchema
from .custom_fields import Statistic, AggregateBy

__all__ = ["AggregateNetworkObjectsSchema", "AggregateNetworkObjectsExposed"]


class AggregateNetworkObjectsExposed(BaseExposedQuery):
    def __init__(self, *, total_network_objects, statistic, aggregate_by):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.total_network_objects = total_network_objects
        self.statistic = statistic
        self.aggregate_by = aggregate_by

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine aggregate_network_objects object.

        Returns
        -------
        Query
        """
        tot_network_objs = self.total_network_objects._flowmachine_query_obj

        return AggregateNetworkObjects(
            total_network_objects=tot_network_objs,
            statistic=self.statistic,
            aggregate_by=self.aggregate_by,
        )


class AggregateNetworkObjectsSchema(BaseSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["aggregate_network_objects"]))
    total_network_objects = fields.Nested(TotalNetworkObjectsSchema, required=True)
    statistic = Statistic()
    aggregate_by = AggregateBy()

    __model__ = AggregateNetworkObjectsExposed
