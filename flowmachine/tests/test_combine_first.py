# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from flowmachine.core.custom_query import CustomQuery
from flowmachine.core.spatial_unit import make_spatial_unit
from flowmachine.core.errors import MissingColumnsError
from flowmachine.features.subscriber.most_frequent_location import MostFrequentLocation
from flowmachine.features.location.flows import Flows
from flowmachine.features.utilities.combine_first import CombineFirst


@pytest.mark.parametrize(
    "join_columns, combine_columns",
    [
        (["location_id_from", "location_id_to"], "value"),
        ("location_id_from", ["location_id_to", "value"]),
    ],
)
def test_combine_first_column_names(
    join_columns, combine_columns, get_column_names_from_run
):
    """Test that CombineFirst's column_names property is accurate"""
    cf = CombineFirst(
        first_query=Flows(
            loc1=MostFrequentLocation(
                "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("cell")
            ),
            loc2=MostFrequentLocation(
                "2016-01-02", "2016-01-03", spatial_unit=make_spatial_unit("cell")
            ),
        ),
        other_query=Flows(
            loc1=MostFrequentLocation(
                "2016-01-02", "2016-01-03", spatial_unit=make_spatial_unit("cell")
            ),
            loc2=MostFrequentLocation(
                "2016-01-03", "2016-01-04", spatial_unit=make_spatial_unit("cell")
            ),
        ),
        join_columns=join_columns,
        combine_columns=combine_columns,
    )
    assert get_column_names_from_run(cf) == cf.column_names


def test_combine_first_overlapping_join_combine_columns_raises():
    """
    Test that CombineFirst raises a ValueError if join_columns overlap with
    combine_columns
    """
    flows = Flows(
        loc1=MostFrequentLocation(
            "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("cell")
        ),
        loc2=MostFrequentLocation(
            "2016-01-02", "2016-01-03", spatial_unit=make_spatial_unit("cell")
        ),
    )
    with pytest.raises(ValueError):
        cf_with_overlapping_columns = CombineFirst(
            first_query=flows,
            other_query=flows,
            join_columns=["location_id_from", "location_id_to"],
            combine_columns=["location_id_to", "value"],
        )


@pytest.mark.parametrize(
    "join_columns, combine_columns",
    [
        ("shared_col", "first_col"),
        ("shared_col", "other_col"),
        ("first_col", "shared_col"),
        ("other_col", "shared_col"),
    ],
)
def test_combine_first_missing_columns_raises(join_columns, combine_columns):
    """
    Test that CombineFirst raises a MissingColumnsError of either query is
    missing any of join_columns or combine_columns
    """
    first_query = CustomQuery(
        sql="SELECT 1 AS first_col, 2 AS shared_col",
        column_names=["first_col", "shared_col"],
    )
    other_query = CustomQuery(
        sql="SELECT 2 AS shared_col, 3 AS other_col",
        column_names=["shared_col", "other_col"],
    )
    with pytest.raises(MissingColumnsError):
        cf_with_missing_columns = CombineFirst(
            first_query=first_query,
            other_query=other_query,
            join_columns=join_columns,
            combine_columns=combine_columns,
        )


def test_combine_first_fills_nulls(get_dataframe):
    """
    Test that null values in rows present in first_query are filled with
    corresponding non-null values from other_query
    """
    first_query = CustomQuery(
        sql="SELECT 'foo' AS key, NULL::text AS value",
        column_names=["key", "value"],
    )
    other_query = CustomQuery(
        sql="SELECT 'foo' AS key, 'bar' AS value",
        column_names=["key", "value"],
    )
    cf = CombineFirst(
        first_query=first_query,
        other_query=other_query,
        join_columns="key",
        combine_columns="value",
    )
    df = get_dataframe(cf)
    expected_df = pd.DataFrame.from_records([("foo", "bar")], columns=["key", "value"])
    assert_frame_equal(df, expected_df)


def test_combine_first_does_not_overwrite_non_nulls(get_dataframe):
    """
    Test that non-null values in first_query arenot overwritten by values from
    other_query
    """
    first_query = CustomQuery(
        sql="SELECT 'foo' AS key, 'bar' AS value",
        column_names=["key", "value"],
    )
    other_query = CustomQuery(
        sql="SELECT 'foo' AS key, 'NOTbar' AS value",
        column_names=["key", "value"],
    )
    cf = CombineFirst(
        first_query=first_query,
        other_query=other_query,
        join_columns="key",
        combine_columns="value",
    )
    df = get_dataframe(cf)
    expected_df = pd.DataFrame.from_records([("foo", "bar")], columns=["key", "value"])
    assert_frame_equal(df, expected_df)


def test_combine_first_fills_missing(get_dataframe):
    """
    Test that CombineFirst contains values from other_query that are missing
    from first_query
    """
    # Make a query with correct columns but empty result
    empty_query = Flows(
        loc1=MostFrequentLocation(
            "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("cell")
        ),
        loc2=MostFrequentLocation(
            "2016-01-02", "2016-01-03", spatial_unit=make_spatial_unit("cell")
        ),
    ).numeric_subset("value", -2, -1)

    other_query = Flows(
        loc1=MostFrequentLocation(
            "2016-01-02", "2016-01-03", spatial_unit=make_spatial_unit("cell")
        ),
        loc2=MostFrequentLocation(
            "2016-01-03", "2016-01-04", spatial_unit=make_spatial_unit("cell")
        ),
    )

    cf = CombineFirst(
        first_query=empty_query,
        other_query=other_query,
        join_columns=["location_id_from", "location_id_to"],
        combine_columns="value",
    )
    assert_frame_equal(get_dataframe(cf), get_dataframe(other_query))


def test_combine_first_keeps_rows_missing_from_other_query(get_dataframe):
    """
    Test that CombineFirst contains values from first_query that are missing
    from other_query
    """
    first_query = Flows(
        loc1=MostFrequentLocation(
            "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("cell")
        ),
        loc2=MostFrequentLocation(
            "2016-01-02", "2016-01-03", spatial_unit=make_spatial_unit("cell")
        ),
    )
    # Make a query with correct columns but empty result
    empty_query = Flows(
        loc1=MostFrequentLocation(
            "2016-01-02", "2016-01-03", spatial_unit=make_spatial_unit("cell")
        ),
        loc2=MostFrequentLocation(
            "2016-01-03", "2016-01-04", spatial_unit=make_spatial_unit("cell")
        ),
    ).numeric_subset("value", -2, -1)

    cf = CombineFirst(
        first_query=first_query,
        other_query=empty_query,
        join_columns=["location_id_from", "location_id_to"],
        combine_columns="value",
    )
    assert_frame_equal(get_dataframe(cf), get_dataframe(first_query))
