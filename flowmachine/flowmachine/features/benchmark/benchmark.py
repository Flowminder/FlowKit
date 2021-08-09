import cProfile, pstats
from asyncio import Future
from typing import Union

import psycopg2 as ps
import flowmachine
from flowmachine.core import CustomQuery
from sqlalchemy.schema import MetaData
from sqlalchemy import Table, Column, Integer, inspect, Text


class BenchmarkQuery(CustomQuery):
    """
    A class that runs a query but does not return the regular output;
    instead, a set of benchmarks are retuned.
    For now, the prototype only returns the time taken to run the query.

    Parameters
    ----------
    sql : str
        An sql query string
    column_names : list of str or set of str
        The column names to return

    """

    def __init__(self, sql : str, column_names):
        super().__init__(sql, column_names)
        self.turn_off_caching()  # For idempotence; we don't want to overestimate by just returning a cached result
        self._profiler = cProfile.Profile()

    def run_benchmark(self) -> float:
        """
        Returns the total time taken to run the function (as measured by cProfiler)

        Returns
        -------
        float

        Notes
        -----
        At present, this function blocks wile the query is run. This
        should probably be amended
        """
        self._profiler.enable()
        future = super().to_sql("benchmark", None, False)
        future.result()
        self._profiler.disable()
        stats = pstats.Stats(self._profiler)
        return stats.total_tt


def test_benchmark():
    """Test for the BenchmarkQuery class"""
    # Set up:
    # Get connection
    flowmachine.connect()
    conn = flowmachine.core.context.get_db()
    eng = conn.engine
    print(eng)

    bench_query = BenchmarkQuery('SELECT * FROM events.calls', ["msisdn"])
    time = bench_query.run_benchmark()
    print(time)
    assert time


if __name__ == "__main__":
    # This should be removed on commit, but I'm keeping it here for now.
    test_benchmark()
