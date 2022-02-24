import cProfile, pstats

from typing import Union


import flowmachine
from flowmachine.core import Query
from flowmachine.core.context import get_db


import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class BenchmarkQuery(Query):
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

    # To get SQL-compatible output from an EXPLAIN statement, we need to store it using EXECUTE
    # From https://stackoverflow.com/questions/7682102/putting-explain-results-into-a-table
    # I really don't like this approach, for a few reasons:
    # - As far as I can tell, this is the only function in Flowkit
    # - Concat-ing to EXECUTE feels like it's introducing a vulnerability
    # I'm using because I can't see a way to do arbitrary benchmarks without building a parallel
    # infrastructure; the advantage of this route is that it takes advantage of the run-poll
    # model that the real queries run on
    _explain_func="""
DROP FUNCTION estimate_cost(text);
CREATE OR REPLACE FUNCTION estimate_cost(
	IN query text, 
	OUT execution_time float,
	OUT planning_time float
)
AS
$BODY$
DECLARE
	query_explain  text;
	explanation    json;
BEGIN
query_explain :=e'EXPLAIN(ANALYZE TRUE, FORMAT JSON) ' || query;
EXECUTE query_explain INTO explanation;
execution_time := explanation->0->>'Execution Time';
planning_time := explanation->0->>'Planning Time';
RETURN;
END;
$BODY$
LANGUAGE plpgsql;
    """

    def __init__(self,
                 benchmark_target : Query,
                 ):
        super().__init__(cache=False)
        self.benchmark_target = benchmark_target

    def _make_query(self):
        # NOTE: Beware the string delimiters here! Making this a bound query or similar would be much better!
        escaped_query = self.benchmark_target.get_query().replace(r"'", r"''")
        return f""" 
        SELECT execution_time, planning_time FROM estimate_cost('{escaped_query}')
        """

    @property
    def column_names(self):
        return ["execution_time", "planning_time"]

    # This can be broken out into its own function
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
        conn = get_db()
        eng = conn.engine
        self._profiler.enable()
        eng.execute("SELECT execution_time, planning_time FROM events.calls")
        self._profiler.disable()
        stats = pstats.Stats(self._profiler)
        return stats.total_tt

    # We need to override _make_sql to define the stored query
    # _before_ we run the rest of the expression
    def _make_sql(self, name:str, schema: Union[str, None] = None):
        query_list = super()._make_sql(name, schema)
        query_list = [self._explain_func] + query_list
        return query_list


