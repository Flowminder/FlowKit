# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Definition of a custom marshmallow field for aggregation units, and function
for getting the corresponding SpatialUnit object.
"""
from marshmallow.fields import String
from marshmallow.validate import OneOf


class AggregationUnitKind(String):
    """
    A string representing an aggregation unit (for example: "admin0", "admin1", "admin2", ...)
    """

    def __init__(self, validate=None, **kwargs):
        if validate is not None:
            raise ValueError(
                "The AggregationUnitKind field provides its own validation "
                "and thus does not accept a 'validate' argument."
            )
        validate = OneOf(["admin0", "admin1", "admin2", "admin3", "lon-lat"])
        super().__init__(validate=validate, **kwargs)

    def _serialize(self, value, attr, obj, **kwargs):
        # This won't serialize the other fields that went into constructing the spatial unit
        # (e.g. 'mapping_table')
        try:
            return value.canonical_name
        except AttributeError:
            return super()._serialize(value, attr, obj, **kwargs)


class AggregationUnitMixin:
    aggregation_unit = AggregationUnitKind(required=True)
    mapping_table = String(required=False, allow_none=True)
    geom_table = String(required=False, allow_none=True)
    geom_table_join_column = String(required=False, allow_none=True)
