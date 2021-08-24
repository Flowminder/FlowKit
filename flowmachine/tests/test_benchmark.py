# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for benchmark functions, including verification of correctness of tables afterwards
"""

import pytest
from flowmachine.features.benchmark.benchmark import BenchmarkQuery
from flowmachine.core.dummy_query import DummyQuery
from typing import List


def test_make_sql():
    action_params = dict(
        query_kind="spatial_aggregate",
        locations=dict(
            query_kind="daily_location",
            date="2016-01-01",
            method="last",
            aggregation_unit="admin3",
        ),
    )
    query = DummyQuery("quartz")
    bench = BenchmarkQuery(query)
    out = bench._make_sql("foo", "cache")
    assert out[0] == bench._explain_func
