# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow_oneofschema import OneOfSchema

from .dummy_query import DummyQuerySchema, DummyQueryExposed
from .daily_location import DailyLocationSchema, DailyLocationExposed
from .modal_location import ModalLocationSchema, ModalLocationExposed
from .flows import FlowsSchema, FlowsExposed
from .meaningful_locations import (
    MeaningfulLocationsAggregateSchema,
    MeaningfulLocationsAggregateExposed,
    MeaningfulLocationsBetweenLabelODMatrixSchema,
    MeaningfulLocationsBetweenLabelODMatrixExposed,
    MeaningfulLocationsBetweenDatesODMatrixSchema,
    MeaningfulLocationsBetweenDatesODMatrixExposed,
)
from .geography import GeographySchema, GeographyExposed
from .location_event_counts import LocationEventCountsSchema, LocationEventCountsExposed

# from .subscriber_locations import SubscriberLocationsSchema, SubscriberLocationsExposed


class FlowmachineQuerySchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "dummy_query": DummyQuerySchema,
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
        "flows": FlowsSchema,
        # "subscriber_locations": SubscriberLocationsSchema
        "meaningful_locations_aggregate": MeaningfulLocationsAggregateSchema,
        "meaningful_locations_between_label_od_matrix": MeaningfulLocationsBetweenLabelODMatrixSchema,
        "meaningful_locations_between_dates_od_matrix": MeaningfulLocationsBetweenDatesODMatrixSchema,
        "geography": GeographySchema,
        "location_event_counts": LocationEventCountsSchema,
    }

    def get_obj_type(self, obj):
        if isinstance(obj, DummyQueryExposed):
            return "dummy_query"
        elif isinstance(obj, DailyLocationExposed):
            return "daily_location"
        elif isinstance(obj, ModalLocationExposed):
            return "modal_location"
        elif isinstance(obj, FlowsExposed):
            return "flows"
        # elif isinstance(obj, SubscriberLocationsExposed):
        #     return "subscriber_locations"
        elif isinstance(obj, MeaningfulLocationsAggregateExposed):
            return "meaningful_locations_aggregate"
        elif isinstance(obj, MeaningfulLocationsBetweenLabelODMatrixExposed):
            return "meaningful_locations_between_label_od_matrix"
        elif isinstance(obj, MeaningfulLocationsBetweenDatesODMatrixExposed):
            return "meaningful_locations_between_dates_od_matrix"
        elif isinstance(obj, GeographyExposed):
            return "geography"
        elif isinstance(obj, LocationEventCountsExposed):
            return "location_event_counts"
        else:
            raise ValueError(
                f"Object type '{obj.__class__.__name__}' not registered in FlowmachineQuerySchema."
            )
