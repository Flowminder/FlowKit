from flowmachine.core.server.query_schemas import BaseExposedQuery
from flowmachine.features import Flows
from flowmachine.features.location.redacted_flows import RedactedFlows


class InflowsExposed(BaseExposedQuery):
    def __init__(self, *, from_location, to_location, join_type):
        self.from_location = from_location
        self.to_location = to_location

    def _flowmachine_query_obj(self):
        loc1 = self.to_location._flowmachine_query_obj
        loc2 = self.from_location._flowmachine_query_obj
        return RedactedFlows(flows=Flows(loc1, loc2, self.join_type)).inflow()


class OutflowsExposed(BaseExposedQuery):
    def __init__(self, *, from_location, to_location, join_type):
        self.from_location = from_location
        self.to_location = to_location

    def _flowmachine_query_obj(self):
        loc1 = self.to_location._flowmachine_query_obj
        loc2 = self.from_location._flowmachine_query_obj
        return RedactedFlows(flows=Flows(loc1, loc2, self.join_type)).outflow()
