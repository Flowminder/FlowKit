"""
Utilities to generate access tokens for testing purposes
"""


query_kinds = ["daily_location", "modal_location", "flow"]


exemplar_query_params = {
    "daily_location": {
        "params": {
            "query_kind": "daily_location",
            "date": "2016-01-01",
            "aggregation_unit": "admin3",
            "method": "last",
        },
        "token": "admin3:daily_location:daily_location",
    },
    "modal_location": {
        "token": [
            "admin3:modal_location:daily_location",
            "admin3:modal_location:modal_location",
        ],
        "params": {
            "query_kind": "modal_location",
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
    },
    "flow": {
        "params": {
            "query_kind": "flow",
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
        "token": "flow.from_location.daily_location.aggregation_unit.admin3&flow.to_location.daily_location.aggregation_unit.admin3",
    },
}
