# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for flowmachine.feature_collection
"""

from flowmachine import feature_collection
from flowmachine.core import CustomQuery
from flowmachine.features import NocturnalEvents, RadiusOfGyration, SubscriberDegree
from flowmachine.features.utilities.feature_collection import (
    feature_collection_from_list_of_classes,
)


def test_collects_metrics():
    """
    Test that we can instantiate flowmachine.feature_collection with list of
    objects.
    """

    start, stop = "2016-01-01", "2016-01-03"
    metrics = [
        RadiusOfGyration(start, stop),
        NocturnalEvents(start, stop),
        SubscriberDegree(start, stop),
    ]
    expected_columns = [
        "subscriber",
        "value_radiusofgyration_0",
        "value_nocturnalevents_1",
        "value_subscriberdegree_2",
    ]
    fc = feature_collection(metrics)
    column_names = fc.column_names
    assert expected_columns == column_names


def test_from_list_of_classes():
    """
    Test that we can instantiate flowmachine.feature_collection with list of
    classes.
    """

    start, stop = "2016-01-01", "2016-01-03"
    metrics = [RadiusOfGyration, NocturnalEvents, SubscriberDegree]
    expected_columns = [
        "subscriber",
        "value_radiusofgyration_0",
        "value_nocturnalevents_1",
        "value_subscriberdegree_2",
    ]
    fc = feature_collection_from_list_of_classes(metrics, start=start, stop=stop)
    column_names = fc.column_names
    assert expected_columns == column_names


def test_dropna(get_length):
    """
    Test that we are able to keep rows with NA values.
    """

    start, stop = "2016-01-01", "2016-01-03"
    msisdn = "1vGR8kp342yxEpwY"
    sql = """
    select 
        msisdn as subscriber,
        2 as val 
    from 
        events.calls 
    where 
        msisdn = '{}' 
    limit 1
    """.format(
        msisdn
    )

    metrics = [CustomQuery(sql, ["subscriber"]), RadiusOfGyration(start, stop)]
    fc = feature_collection(metrics, dropna=False)

    # usully without dropna=False this query would only return
    # a single row. We check that this is not the case.
    assert get_length(fc) > 1
