import itertools
import zmq
import textwrap
import time

from sqlalchemy import inspect


def poll_until_done(port, query_id, max_tries=100):
    """
    Send zmq message to flowmachine on port `port` which polls the
    query with id `query_id` until the return status is "completed".
    """
    msg_poll_query = {
        "action": "poll_query",
        "params": {"query_kind": "daily_location", "query_id": query_id},
        "request_id": "DUMMY_ID",
    }

    for i in itertools.count():
        if i > max_tries:
            raise RuntimeError("Timeout reached but query is not done. Aborting.")
        print(f"[DDD] Polling query {query_id}...")
        reply = send_message_and_get_reply(port, msg_poll_query)
        if "completed" == reply["status"]:
            break
        time.sleep(0.1)


# TODO: This is almost identical to the new helper function `send_message_and_receive_reply` in flowmachine.core.server.utils
#       We should probably remove the helper function here and switch the tests to using the other.
def send_message_and_get_reply(zmq_url, msg):
    """
    Send message `msg` to zeromq instance on the given port and return the reply.

    Parameters
    ----------
    zmq_url : str
        Connection string for zeromq. Example: "tcp://localhost:5555"
    msg : dict
        JSON representation of the message to be sent.

    Returns
    -------
    dict
        Reply received from zeromq.
    """
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.connect(zmq_url)
    print(f"[DDD] Sending message to zeromq at {zmq_url}")
    socket.send_json(msg)
    reply = socket.recv_json()
    print(f"[DDD] Received reply: {reply}")
    return reply


def get_cache_tables(fm_conn, exclude_internal_tables=True):
    """
    Return any tables present in the cache schema in flowdb.
    If `exclude_internal_tables` is True, the two internal
    tables 'cache.cached' and 'cache.dependencies' are not
    returned, otherwise they are included in the result.
    """
    insp = inspect(fm_conn.engine)
    cache_tables = insp.get_table_names(schema="cache")
    if exclude_internal_tables:
        cache_tables.remove("cached")
        cache_tables.remove("dependencies")
        cache_tables.remove("cache_config")
    return sorted(cache_tables)


def cache_schema_is_empty(fm_conn, check_internal_tables_are_empty=True):
    """
    Return True if the cache schema in flowdb is empty.

    If `check_internal_tables_are_empty` is True then in addition to checking
    that no tables for cached queries exist, it is also checked that the two
    internal tables 'cache.cached' and 'cache.dependencies' are empty.
    """
    insp = inspect(fm_conn.engine)
    cache_tables = insp.get_table_names(schema="cache")

    # Check that there are no cached tables except the flowdb-internal ones
    if cache_tables != ["cache_config", "cached", "dependencies"]:
        return False

    if check_internal_tables_are_empty:
        # Check that cache.cached and cache.dependencies are empty
        res1 = fm_conn.engine.execute("SELECT COUNT(*) FROM cache.cached")
        res2 = fm_conn.engine.execute("SELECT COUNT(*) FROM cache.dependencies")
        if res1.fetchone()[0] != 0 or res2.fetchone()[0] != 0:
            return False

    return True


def create_flowdb_version_table(conn):
    """
    Create the table 'flowdb_version' in the flowdb instance accessible via 'conn'.
    Also create the associated function `flowdb_version()`.
    """
    sql = textwrap.dedent(
        """
        CREATE TABLE IF NOT EXISTS flowdb_version (
            version TEXT PRIMARY KEY,
            release_date DATE,
            updated BOOL
        );
        INSERT INTO flowdb_version (version, release_date)
            VALUES ('0.0.0', '9999-12-31')
              ON CONFLICT (version)
              DO UPDATE SET updated = True;


        CREATE OR REPLACE FUNCTION flowdb_version()
            RETURNS TABLE (
                version TEXT,
                release_date DATE
            ) AS
        $$
        BEGIN
            RETURN QUERY
                SELECT
                    A.version,
                    A.release_date
                FROM flowdb_version AS A;
        END;
        $$  LANGUAGE plpgsql IMMUTABLE
            SECURITY DEFINER
            -- Set a secure search_path: trusted schema(s), then 'pg_temp'.
            SET search_path = public, pg_temp;
        """
    )
    conn.execute(sql)


def create_flowdb_function_available_tables(conn):
    """
    Create the table 'available tables', as well as the function 'available_tables()',
    in the flowdb instance accessible via 'conn'.
    """
    sql = textwrap.dedent(
        """
        CREATE TABLE IF NOT EXISTS available_tables (
            table_name TEXT PRIMARY KEY,
            has_locations BOOL DEFAULT False,
            has_subscribers BOOL DEFAULT False,
            has_counterparts BOOL DEFAULT False
        );
        INSERT INTO available_tables (table_name)
            (SELECT tablename
                FROM pg_tables
                   WHERE NOT EXISTS (
                    SELECT c.relname AS child
                    FROM
                        pg_inherits JOIN pg_class AS c ON (inhrelid=c.oid)
                        JOIN pg_class as p ON (inhparent=p.oid)
                        WHERE c.relname=tablename
                        ) AND
                    schemaname='events')
              ON CONFLICT (table_name)
              DO NOTHING;

        CREATE OR REPLACE FUNCTION available_tables()
            RETURNS TABLE (
                table_name TEXT,
                has_locations Boolean,
                has_subscribers Boolean,
                has_counterparts Boolean
            ) AS
        $$
        BEGIN
            RETURN QUERY
                SELECT
                    A.table_name,
                    A.has_locations,
                    A.has_subscribers,
                    A.has_counterparts
                FROM available_tables AS A;
        END;
        $$  LANGUAGE plpgsql IMMUTABLE
            SECURITY DEFINER
            -- Set a secure search_path: trusted schema(s), then 'pg_temp'.
            SET search_path = public, pg_temp;
        """
    )
    conn.execute(sql)
