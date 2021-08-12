import cProfile, pstats
from asyncio import Future
from typing import Union

import psycopg2 as ps
import flowmachine
from flowmachine.core import Query
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from sqlalchemy.schema import MetaData
from sqlalchemy import Table, Column, Integer, inspect, Text


class BenchmarkQuery():
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

    def __init__(self):
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
        flowmachine.connect()
        conn = flowmachine.core.context.get_db()
        eng = conn.engine
        self._profiler.enable()
        eng.execute("SELECT * FROM events.calls")
        self._profiler.disable()
        stats = pstats.Stats(self._profiler)
        return stats.total_tt


class BenchmarkSchema(BaseSchema):
    pass


def test_benchmark():
    """Test for the BenchmarkQuery class"""
    # Set up:
    # Get connection
    

    bench_query = BenchmarkQuery()
    time = bench_query.run_benchmark()
    print(time)
    assert time


if __name__ == "__main__":
    test_benchmark()