# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, post_load

from flowmachine.core import make_spatial_unit


class BaseSchema(Schema):
    @post_load
    def remove_query_kind_if_present_and_load(self, params, **kwargs):
        # Load any aggregation unit
        aggregation_unit_string = params.pop("aggregation_unit", None)
        mapping_table = params.pop("mapping_table", None)
        geom_table_join_on = params.pop("geom_table_join_column", None)
        geom_table = params.pop("geom_table", None)
        if aggregation_unit_string is not None:
            if "admin" in aggregation_unit_string:
                level = int(aggregation_unit_string[-1])
                spatial_unit_args = {"spatial_unit_type": "admin", "level": level}
            elif "lon-lat" in aggregation_unit_string:
                spatial_unit_args = {
                    "spatial_unit_type": "lon-lat",
                    "geom_table": geom_table,
                    "geom_table_join_on": geom_table_join_on,
                }
            else:
                raise NotImplementedError(
                    f"Aggregation units of type '{aggregation_unit_string}' are not supported at this time."
                )
            params["aggregation_unit"] = make_spatial_unit(
                **spatial_unit_args, mapping_table=mapping_table
            )
        # Strip off query kind if present, because this isn't always wrapped in a OneOfSchema
        return self.__model__(
            **{
                param_name: param_value
                for param_name, param_value in params.items()
                if param_name != "query_kind"
            }
        )
