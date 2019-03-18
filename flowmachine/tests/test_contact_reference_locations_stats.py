# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.contact_reference_locations_stats import *

import pytest
from flowmachine.core import CustomQuery
from flowmachine.utils import list_of_dates
from flowmachine.features import daily_location, ContactBalance


@pytest.mark.parametrize(
    "statistic,msisdn,level,want",
    [
        ("avg", "gwAynWXp4eWvxGP7", "versioned-cell", 298.7215),
        ("avg", "gwAynWXp4eWvxGP7", "versioned-site", 298.7215),
        ("avg", "gwAynWXp4eWvxGP7", "lat-lon", 298.7215),
        ("stddev", "V7MBRewnwQGE91gY", "versioned-cell", 182.519_128),
    ],
)
def test_contact_reference_location_stats(
    get_dataframe, statistic, msisdn, level, want
):
    """ Test a few hand-picked ContactReferenceLocationStats. """
    cb = ContactBalance("2016-01-01", "2016-01-03")
    ml = ModalLocation(
        *[
            daily_location(
                d,
                level=level,
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
    """ Test ContactReferenceLocationStats with custom geometry column. """
    cb = ContactBalance("2016-01-01", "2016-01-03")
    ml = ModalLocation(
        *[
            daily_location(
                d,
                level="versioned-cell",
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


@pytest.mark.parametrize("kwarg", ["statistic", "contact_locations"])
def test_contact_reference_location_stats_errors(kwarg):
    """ Test ValueError is raised for non-compliant kwarg in ContactModalLocationDistance. """

    cb = ContactBalance("2016-01-01", "2016-01-03")

    if kwarg == "contact_locations":
        ml = ModalLocation(
            *[
                daily_location(
                    d,
                    level="admin3",
                    subscriber_subset=cb.counterparts_subset(include_subscribers=True),
                )
                for d in list_of_dates("2016-01-01", "2016-01-03")
            ]
        )
        with pytest.raises(ValueError):
            query = ContactReferenceLocationStats(cb, ml)
        # by encapsulating ModalLocations in a CustomQuery we remove the level attribute from it which should raise an error
        ml = ModalLocation(
            *[
                daily_location(
                    d,
                    level="versioned-cell",
                    subscriber_subset=cb.counterparts_subset(include_subscribers=True),
                )
                for d in list_of_dates("2016-01-01", "2016-01-03")
            ]
        )
        ml = CustomQuery(ml.get_query(), ml.column_names)
        with pytest.raises(ValueError):
            query = ContactReferenceLocationStats(cb, ml)
    else:
        ml = ModalLocation(
            *[
                daily_location(
                    d,
                    level="versioned-cell",
                    subscriber_subset=cb.counterparts_subset(include_subscribers=True),
                )
                for d in list_of_dates("2016-01-01", "2016-01-03")
            ]
        )
        kwargs = {kwarg: "error"}
        with pytest.raises(ValueError):
            query = ContactReferenceLocationStats(cb, ml, **kwargs)
