# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for flowmachine.FeatureCollection
"""

from unittest import TestCase
from flowmachine import FeatureCollection
from flowmachine.core import CustomQuery
from flowmachine.features import RadiusOfGyration, NocturnalCalls, SubscriberDegree


def test_collects_metrics():
    """
    Test that we can instantiate flowmachine.FeatureCollection with list of
    objects.
    """

    start, stop = "2016-01-01", "2016-01-03"
    metrics = [
        RadiusOfGyration(start, stop),
        NocturnalCalls(start, stop),
        SubscriberDegree(start, stop),
    ]
    expected_columns = [
        "subscriber",
        "rog_radiusofgyration_0",
        "percentage_nocturnal_nocturnalcalls_1",
        "degree_subscriberdegree_2",
    ]
    fc = FeatureCollection(metrics)
    column_names = fc.column_names
    assert expected_columns == column_names


def test_from_list_of_classes():
    """
    Test that we can instantiate flowmachine.FeatureCollection with list of
    classes.
    """

    start, stop = "2016-01-01", "2016-01-03"
    metrics = [RadiusOfGyration, NocturnalCalls, SubscriberDegree]
    expected_columns = [
        "subscriber",
        "rog_radiusofgyration_0",
        "percentage_nocturnal_nocturnalcalls_1",
        "degree_subscriberdegree_2",
    ]
    fc = FeatureCollection.from_list_of_classes(metrics, start=start, stop=stop)
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

    metrics = [CustomQuery(sql), RadiusOfGyration(start, stop)]
    fc = FeatureCollection(metrics, dropna=False)

    # usully without dropna=False this query would only return
    # a single row. We check that this is not the case.
    assert get_length(fc) > 1
