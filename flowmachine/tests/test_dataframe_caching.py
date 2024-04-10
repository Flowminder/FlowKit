# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pandas as pd
import pytest

from flowmachine.features import EventTableSubset

# TODO: These tests are for the in memory cache of recently retrieved dataframes once flowmachine is no longer used to get dataframes they should be removed


def test_turn_off_caching():
    """
    *.turn_off_caching() 'forgets' generated dataframe.
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    sd.get_dataframe()
    sd.turn_off_caching()
    with pytest.raises(AttributeError):
        sd._df


def test_turn_off_caching_handles_error():
    """
    *.turn_off_caching() works even if ._df attribute is not present.
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    sd.get_dataframe()
    sd.turn_off_caching()
    sd.turn_on_caching()
    sd.get_dataframe()

    del sd._df
    sd.turn_off_caching()


def test_get_df_without_caching():
    """
    *.get_dataframe() can still retrieve the dataframe without caching.
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    sd.get_dataframe()
    sd.turn_off_caching()
    assert isinstance(sd.get_dataframe(), pd.DataFrame)
    assert isinstance(sd.get_dataframe(), pd.DataFrame)


def test_turn_on_caching():
    """
    *.get_dataframe() dataframe is retained when we turning on caching.
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    sd.get_dataframe()
    sd.turn_off_caching()
    sd.turn_on_caching()
    sd.get_dataframe()
    assert isinstance(sd._df, pd.DataFrame)


def test_cache_is_returned():
    """
    Cache property is returned when called.
    """
    sd = EventTableSubset(start="2016-01-01", stop="2016-01-02")
    sd.get_dataframe()
    sd.turn_on_caching()
    assert sd.cache

    sd.turn_off_caching()
    assert not sd.cache
