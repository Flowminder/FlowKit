# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, post_load


class BaseSchema(Schema):
    @post_load
    def remove_query_kind_if_present_and_load(self, params, **kwargs):
        # Strip off query kind if present, because this isn't always wrapped in a OneOfSchema
        return self.__model__(
            **{
                param_name: param_value
                for param_name, param_value in params.items()
                if param_name != "query_kind"
            }
        )
