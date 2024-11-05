# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas.flows import FlowsExposed, FlowsSchema
from flowmachine.features import Flows
from flowmachine.features.location.redacted_in_out_flows import RedactedInOutFlow


class OutflowsExposed(FlowsExposed):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "outflows"

    @property
    def _flowmachine_query_obj(self):
        loc1 = self.from_location._flowmachine_query_obj
        loc2 = self.to_location._flowmachine_query_obj
        return RedactedInOutFlow(
            in_out_flows=Flows(loc1, loc2, join_type=self.join_type).outflow()
        )


class OutflowsSchema(FlowsSchema):
    __model__ = OutflowsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
