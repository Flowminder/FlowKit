# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow_oneofschema import OneOfSchema


class OneOfQuerySchema(OneOfSchema):
    """
    Schema for deserialising to one of a choice of query kinds.

    The `query_schemas` attribute is an iterable of allowed query schemas.
    """

    type_field = "query_kind"
    type_field_remove = False
    query_schemas = ()

    @property
    def type_schemas(self):
        return {schema.__model__.query_kind: schema for schema in self.query_schemas}

    def get_obj_type(self, obj):
        return obj.query_kind
