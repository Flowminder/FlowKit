# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import Table, make_spatial_unit
from flowmachine.features import SubscriberLocationSubset, UniqueSubscribers


def test_subscriber_location_subset_column_names(exemplar_spatial_unit_param):
    ss = SubscriberLocationSubset(
        "2016-01-01",
        "2016-01-07",
        min_calls=1,
        spatial_unit=exemplar_spatial_unit_param,
    )
    assert ss.head(0).columns.tolist() == ss.column_names


def test_subscribers_make_atleast_one_call_in_admin0():
    """
    The set of subscribers who make at least one call within admin0 over
    whole test time period should be equal to set of unique subscribers
    in test calls table.
    """

    start, stop = "2016-01-01", "2016-01-07"

    sls = SubscriberLocationSubset(
        start, stop, min_calls=1, spatial_unit=make_spatial_unit("admin", level=0)
    )
    us = UniqueSubscribers(start, stop, tables="events.calls")

    sls_subs = set(sls.get_dataframe()["subscriber"])
    us_subs = set(us.get_dataframe()["subscriber"])

    assert sls_subs == us_subs


def test_subscribers_who_make_atleast_3_calls_in_central_development_region():
    """
    Test that we can find subsets for multiple geometries at same time. Will
    find subscribers who have made at least 2 calls in any of the admin2 regions
    within Central Development admin1 region.
    """
    start, stop = "2016-01-01", "2016-01-07"
    regions = Table("admin2", "geography").subset(
        "admin1name", ["Central Development Region"]
    )

    sls = SubscriberLocationSubset(
        start,
        stop,
        min_calls=2,
        spatial_unit=make_spatial_unit(
            "polygon", region_id_column_name="admin2pcod", geom_table=regions
        ),
    )

    df = sls.get_dataframe()

    # we have results for multiple regions
    assert len(df.admin2pcod.unique()) > 1

    # some users should have have made at least 2 calls in more than one region
    # and should therefore appear twice
    assert len(df[df.duplicated("subscriber")]) > 0
