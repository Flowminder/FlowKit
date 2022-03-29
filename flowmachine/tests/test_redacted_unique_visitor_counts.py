# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    SubscriberLocations,
    daily_location,
    UniqueSubscriberCounts,
)
from flowmachine.features.location.active_at_reference_location_counts import (
    ActiveAtReferenceLocationCounts,
)
from flowmachine.features.location.redacted_unique_vistor_counts import (
    RedactedUniqueVisitorCounts,
)
from flowmachine.features.location.unique_visitor_counts import UniqueVisitorCounts
from flowmachine.features.subscriber.active_at_reference_location import (
    ActiveAtReferenceLocation,
)
from flowmachine.features.subscriber.unique_locations import UniqueLocations


def test_redacted_unique_visitor_counts_column_names(get_column_names_from_run):
    assert get_column_names_from_run(
        RedactedUniqueVisitorCounts(
            unique_visitor_counts=UniqueVisitorCounts(
                ActiveAtReferenceLocationCounts(
                    ActiveAtReferenceLocation(
                        subscriber_locations=UniqueLocations(
                            SubscriberLocations(
                                "2016-01-01",
                                "2016-01-02",
                                spatial_unit=make_spatial_unit("admin", level=3),
                            )
                        ),
                        reference_locations=daily_location("2016-01-03"),
                    )
                ),
                UniqueSubscriberCounts(
                    "2016-01-01",
                    "2016-01-02",
                    spatial_unit=make_spatial_unit("admin", level=3),
                ),
            )
        )
    ) == ["pcod", "value"]


def test_redacted_unique_visitor_counts(get_dataframe):
    """
    Values test for redacted unique visitor counts.
    """
    activity = RedactedUniqueVisitorCounts(
        unique_visitor_counts=UniqueVisitorCounts(
            ActiveAtReferenceLocationCounts(
                ActiveAtReferenceLocation(
                    subscriber_locations=UniqueLocations(
                        SubscriberLocations(
                            "2016-01-01",
                            "2016-01-02",
                            spatial_unit=make_spatial_unit("admin", level=3),
                        )
                    ),
                    reference_locations=daily_location("2016-01-03"),
                )
            ),
            UniqueSubscriberCounts(
                "2016-01-01",
                "2016-01-02",
                spatial_unit=make_spatial_unit("admin", level=3),
            ),
        )
    )
    df = get_dataframe(activity).set_index("pcod")
    assert len(df) == 2
    assert df.loc["524 3 08 44"].value == 170
