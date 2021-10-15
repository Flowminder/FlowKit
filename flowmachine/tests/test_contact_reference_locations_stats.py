# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.contact_reference_locations_stats import *

import pytest
from flowmachine.core import CustomQuery, make_spatial_unit
from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.utils import list_of_dates
from flowmachine.features import daily_location, ContactBalance, ModalLocation


@pytest.mark.parametrize(
    "statistic,msisdn,spatial_unit_type,want",
    [
        ("avg", "gwAynWXp4eWvxGP7", "versioned-cell", 298.7215),
        ("avg", "gwAynWXp4eWvxGP7", "versioned-site", 298.7215),
        ("avg", "gwAynWXp4eWvxGP7", "lon-lat", 298.7215),
        ("stddev", "V7MBRewnwQGE91gY", "versioned-cell", 182.519_128),
    ],
)
def test_contact_reference_location_stats(
    get_dataframe, statistic, msisdn, spatial_unit_type, want
):
    """Test a few hand-picked ContactReferenceLocationStats."""
    cb = ContactBalance("2016-01-01", "2016-01-03")
    ml = ModalLocation(
        *[
            daily_location(
                d,
                spatial_unit=make_spatial_unit(spatial_unit_type),
                subscriber_subset=cb.counterparts_subset(include_subscribers=True),
            )
            for d in list_of_dates("2016-01-01", "2016-01-03")
        ]
    )
    cb.store()
    ml.store()
    query = ContactReferenceLocationStats(cb, ml, statistic=statistic)
    df = get_dataframe(query).set_index("subscriber")
    assert df.value[msisdn] == pytest.approx(want)


def test_contact_reference_location_stats_custom_geometry(get_dataframe):
    """Test ContactReferenceLocationStats with custom geometry column."""
    cb = ContactBalance("2016-01-01", "2016-01-03")
    ml = ModalLocation(
        *[
            daily_location(
                d,
                spatial_unit=make_spatial_unit("versioned-cell"),
                subscriber_subset=cb.counterparts_subset(include_subscribers=True),
            )
            for d in list_of_dates("2016-01-01", "2016-01-03")
        ]
    )
    cb.store()
    ml.store()
    ml = CustomQuery(
        f"SELECT subscriber, ST_POINT(lon, lat) AS loc FROM ({ml.get_query()}) _",
        ["subscriber", "loc"],
    )
    query = ContactReferenceLocationStats(cb, ml, statistic="avg", geom_column="loc")
    df = get_dataframe(query).set_index("subscriber")
    assert df.value["gwAynWXp4eWvxGP7"] == pytest.approx(298.7215)


def test_contact_reference_location_stats_false_statistic_raises():
    """Test ValueError is raised for non-compliant statistics parameter."""
    cb = ContactBalance("2016-01-01", "2016-01-03")
    ml = ModalLocation(
        *[
            daily_location(
                d,
                spatial_unit=make_spatial_unit("versioned-cell"),
                subscriber_subset=cb.counterparts_subset(include_subscribers=True),
            )
            for d in list_of_dates("2016-01-01", "2016-01-03")
        ]
    )
    with pytest.raises(ValueError):
        query = ContactReferenceLocationStats(cb, ml, statistic="error")


def test_contact_reference_location_bad_spatial_unit_raises():
    """
    Test InvalidSpatialUnitError is raised for contact_location with
    non-compliant spatial unit.
    """
    cb = ContactBalance("2016-01-01", "2016-01-03")
    ml = ModalLocation(
        *[
            daily_location(
                d,
                spatial_unit=make_spatial_unit("admin", level=3),
                subscriber_subset=cb.counterparts_subset(include_subscribers=True),
            )
            for d in list_of_dates("2016-01-01", "2016-01-03")
        ]
    )
    with pytest.raises(InvalidSpatialUnitError):
        query = ContactReferenceLocationStats(cb, ml)


def test_contact_reference_location_no_spatial_unit_raises():
    """Test ValueError is raised for contact_location without spatial_unit attribute."""
    cb = ContactBalance("2016-01-01", "2016-01-03")
    # by encapsulating ModalLocations in a CustomQuery we remove the spatial_unit
    # attribute from it which should raise an error
    ml = ModalLocation(
        *[
            daily_location(
                d,
                spatial_unit=make_spatial_unit("versioned-cell"),
                subscriber_subset=cb.counterparts_subset(include_subscribers=True),
            )
            for d in list_of_dates("2016-01-01", "2016-01-03")
        ]
    )
    ml = CustomQuery(ml.get_query(), ml.column_names)
    with pytest.raises(ValueError):
        query = ContactReferenceLocationStats(cb, ml)


def test_contact_reference_location_no_subscriber_raises():
    """Test ValueError is raised for contact_location without subscriber."""
    cb = ContactBalance("2016-01-01", "2016-01-03")
    cl = CustomQuery("SELECT 1 AS foo", ["foo"])
    with pytest.raises(ValueError):
        query = ContactReferenceLocationStats(cb, cl)
