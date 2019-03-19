from marshmallow_oneofschema import OneOfSchema

from .dummy_query import DummyQuerySchema, DummyQueryExposed
from .daily_location import DailyLocationSchema, DailyLocationExposed
from .modal_location import ModalLocationSchema, ModalLocationExposed
from .meaningful_locations import (
    MeaningfulLocationsAggregateSchema,
    MeaningfulLocationsAggregateExposed,
    MeaningfulLocationsBetweenLabelODMatrixSchema,
    MeaningfulLocationsBetweenLabelODMatrixExposed,
)

# from .subscriber_locations import SubscriberLocationsSchema, SubscriberLocationsExposed


class FlowmachineQuerySchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "dummy_query": DummyQuerySchema,
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
        # "subscriber_locations": SubscriberLocationsSchema
        "meaningful_locations_aggregate": MeaningfulLocationsAggregateSchema,
        "meaningful_locations_od_matrix": MeaningfulLocationsBetweenLabelODMatrixSchema,
    }

    def get_obj_type(self, obj):
        if isinstance(obj, DummyQueryExposed):
            return "dummy_query"
        elif isinstance(obj, DailyLocationExposed):
            return "daily_location"
        elif isinstance(obj, ModalLocationExposed):
            return "modal_location"
        # elif isinstance(obj, SubscriberLocationsExposed):
        #     return "subscriber_locations"
        elif isinstance(obj, MeaningfulLocationsAggregateExposed):
            return "meaningful_locations_aggregate"
        else:
            raise ValueError(
                f"Object type '{obj.__class__.__name__}' not registered in FlowmachineQuerySchema."
            )
