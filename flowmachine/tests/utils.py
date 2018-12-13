# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Functionality to make tests easier and less repetitive.
"""

levels = [
    "cell",
    "versioned-cell",
    "versioned-site",
    "lat-lon",
    "grid",
    "polygon",
    "admin3",
]
kwargs = [
    {},
    {},
    {},
    {},
    {"size": 50},
    {"column_name": "admin3pcod", "polygon_table": "geography.admin3"},
    {},
]
# This is a common list that will often be used
subscriber_plus_levels = [
    ["subscriber", "location_id"],
    ["subscriber", "location_id", "version", "lon", "lat"],
    ["subscriber", "site_id", "version", "lon", "lat"],
    ["subscriber", "lat", "lon"],
    ["subscriber", "grid_id"],
    ["subscriber", "admin3pcod"],
    ["subscriber", "pcod"],
]

levels_only = [
    ["location_id"],
    ["location_id", "version", "lon", "lat"],
    ["site_id", "version", "lon", "lat"],
    ["lat", "lon"],
    ["grid_id"],
    ["admin3pcod"],
    ["pcod"],
]


def sweep_levels_assert_columns(
    cls,
    common_kwargs,
    levels=levels,
    kwarg_list=kwargs,
    expected_cols=subscriber_plus_levels,
    order_matters=True,
):
    """
    Takes a class and repeated calls it with a different levels and assert that the resultant
    columns are as expected.
    Inputs
    ======
    cls:
        a flowmachine.Query class that takes a level argument
    common_kwargs:
        A dictionary of keyword arguments that all instances of the
        class needs to take.
    levels: list of strings, default (see above)
        List of the levels to sweep over
    kwargs_list: default (see above)
        Keyword arguments specific to each level as a list.
    expected_cols: default (see above)
        list of list of column names that are expected as output.
    order_matters: bool, default True
        If False just check that the lists contain the same elements (but
        not necessarily in the same order), otherwise check the order as well.
    """

    assert len(levels) == len(kwarg_list) == len(expected_cols)

    for level, k, ecs in zip(levels, kwargs, expected_cols):
        obj = cls(level=level, **common_kwargs, **k)
        cols = obj.column_names
        if order_matters:
            assert cols == ecs
        else:
            assert len(cols) == len(ecs)
            assert set(cols) == set(ecs)
