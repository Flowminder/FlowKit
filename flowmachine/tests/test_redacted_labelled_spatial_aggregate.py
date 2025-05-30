# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

import pandas as pd
from pandas.testing import assert_frame_equal

from flowmachine.core import make_spatial_unit
from flowmachine.features import SubscriberHandsetCharacteristic
from flowmachine.features.location.labelled_spatial_aggregate import (
    LabelledSpatialAggregate,
)
from flowmachine.features.location.redacted_labelled_spatial_aggregate import (
    RedactedLabelledSpatialAggregate,
)
from flowmachine.features.subscriber.daily_location import locate_subscribers


def test_redaction(get_dataframe):
    locations = locate_subscribers(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=3),
        method="most-common",
    )
    metric = SubscriberHandsetCharacteristic(
        "2016-01-01", "2016-01-02", characteristic="hnd_type"
    )
    labelled = LabelledSpatialAggregate(locations=locations, labels=metric)

    redacted = RedactedLabelledSpatialAggregate(labelled_spatial_aggregate=labelled)
    redacted_df = get_dataframe(redacted)

    target = pd.DataFrame(
        [
            ["524 3 08 44", "Feature", 37],
            ["524 3 08 44", "Smart", 31],
            ["524 4 12 62", "Feature", 40],
            ["524 4 12 62", "Smart", 26],
        ],
        columns=["pcod", "value_label", "value"],
    )

    assert_frame_equal(redacted_df, target)


def test_spatial_unit_validation():
    with pytest.raises(ValueError, match="spatial unit"):
        RedactedLabelledSpatialAggregate(
            labelled_spatial_aggregate=SubscriberHandsetCharacteristic(
                "2016-01-01", "2016-01-02", characteristic="hnd_type"
            )
        )
