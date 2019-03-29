import json
import pytest

from flowmachine.core.available_dates import AvailableDates


@pytest.mark.parametrize(
    "event_types, expected_result",
    [
        (
            ["calls"],
            [
                {
                    "event_type": "calls",
                    "dates": [
                        "2016-01-01",
                        "2016-01-02",
                        "2016-01-03",
                        "2016-01-04",
                        "2016-01-05",
                        "2016-01-06",
                        "2016-01-07",
                        "2016-09-09",
                    ],
                }
            ],
        ),
        (
            ["calls", "sms", "topups"],
            [
                {
                    "event_type": "calls",
                    "dates": [
                        "2016-01-01",
                        "2016-01-02",
                        "2016-01-03",
                        "2016-01-04",
                        "2016-01-05",
                        "2016-01-06",
                        "2016-01-07",
                        "2016-09-09",
                    ],
                },
                {
                    "event_type": "sms",
                    "dates": [
                        "2016-01-01",
                        "2016-01-02",
                        "2016-01-03",
                        "2016-01-04",
                        "2016-01-05",
                        "2016-01-06",
                        "2016-01-07",
                    ],
                },
                {
                    "event_type": "topups",
                    "dates": [
                        "2016-01-01",
                        "2016-01-02",
                        "2016-01-03",
                        "2016-01-04",
                        "2016-01-05",
                        "2016-01-06",
                        "2016-01-07",
                        "2016-09-09",
                    ],
                },
            ],
        ),
    ],
)
def test_available_dates(event_types, expected_result, get_dataframe):
    q = AvailableDates(event_types=event_types)
    result = json.loads(get_dataframe(q).to_json(orient="records"))
    assert expected_result == result
