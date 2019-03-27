import logging
import structlog

__all__ = ["query_run_log"]

# Logger for all queries run or accessed
query_run_log = logging.getLogger("flowmachine-server")
query_run_log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
query_run_log.addHandler(ch)
query_run_log = structlog.wrap_logger(query_run_log)
