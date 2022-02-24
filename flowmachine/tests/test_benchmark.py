# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for benchmark functions, including verification of correctness of tables afterwards
"""

import pytest
from flowmachine.features.benchmark.benchmark import run_benchmark


@pytest.mark.usefixtures("create_test_tables")
def test_run_benchmark(cursor, test_tables):
    """run_benchmark should produce a set of metrics, but leave the database untouched afterwards"""
    initial_hash = hash(test_tables)
    bench = run_benchmark(cursor)
    final_hash = hash(test_tables)
    assert initial_hash == final_hash
    assert bench
