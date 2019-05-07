"""
Utilities to generate access tokens for testing purposes
"""


query_kinds = ["daily_location", "modal_location", "flow"]
permissions_types = ["run", "poll", "get_result"]
aggregation_types = ["admin0", "admin1", "admin2", "admin3", "admin4"]


exemplar_query_params = {
    "daily_location": {
        "query_kind": "daily_location",
        "date": "2016-01-01",
        "aggregation_unit": "admin3",
        "method": "last",
    },
    "modal_location": {
        "query_kind": "modal_location",
        "aggregation_unit": "admin3",
        "locations": [
            {
                "query_kind": "daily_location",
                "date": "2016-01-01",
                "aggregation_unit": "admin3",
                "method": "last",
            },
            {
                "query_kind": "daily_location",
                "date": "2016-01-02",
                "aggregation_unit": "admin3",
                "method": "last",
            },
        ],
    },
    "flow": {
        "query_kind": "flow",
        "aggregation_unit": "admin3",
        "from_location": {
            "query_kind": "daily_location",
            "date": "2016-01-01",
            "aggregation_unit": "admin3",
            "method": "last",
        },
        "to_location": {
            "query_kind": "daily_location",
            "date": "2016-01-02",
            "aggregation_unit": "admin3",
            "method": "last",
        },
    },
}
