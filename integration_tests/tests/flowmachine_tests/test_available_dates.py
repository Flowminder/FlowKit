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
                        "20160101",
                        "20160102",
                        "20160103",
                        "20160104",
                        "20160105",
                        "20160106",
                        "20160107",
                        "20160909",
                    ],
                }
            ],
        ),
        (
            ["calls", "sms", "topups"],
            [
                {
                    "dates": [
                        "20160101",
                        "20160102",
                        "20160103",
                        "20160104",
                        "20160105",
                        "20160106",
                        "20160107",
                        "20160909",
                    ],
                    "event_type": "calls",
                },
                {
                    "dates": [
                        "20160101",
                        "20160102",
                        "20160103",
                        "20160104",
                        "20160105",
                        "20160106",
                        "20160107",
                    ],
                    "event_type": "sms",
                },
                {
                    "dates": [
                        "20160101",
                        "20160102",
                        "20160103",
                        "20160104",
                        "20160105",
                        "20160106",
                        "20160107",
                        "20160909",
                    ],
                    "event_type": "topups",
                },
            ],
        ),
    ],
)
def test_available_dates(event_types, expected_result, get_dataframe):
    q = AvailableDates(event_types=event_types)
    result = json.loads(get_dataframe(q).to_json(orient="records"))
    assert expected_result == result
