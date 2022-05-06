from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas.flows import FlowsExposed, FlowsSchema
from flowmachine.features import Flows
from flowmachine.features.location.redacted_in_out_flows import RedactedInOutFlow


class InflowsExposed(FlowsExposed):
    @property
    def _flowmachine_query_obj(self):
        loc1 = self.from_location._flowmachine_query_obj
        loc2 = self.to_location._flowmachine_query_obj
        return RedactedInOutFlow(
            in_out_flows=Flows(loc1, loc2, join_type=self.join_type).inflow()
        )


class InflowsSchema(FlowsSchema):
    query_kind = fields.String(validate=OneOf(["inflows"]))
    __model__ = InflowsExposed
