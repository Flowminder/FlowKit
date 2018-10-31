/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/******************************************************************************
### flowdb_version ###

Returns the `flowdb` version.

******************************************************************************/
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


/******************************************************************************
### available_tables ###

Returns the `flowdb` version.

******************************************************************************/
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


/******************************************************************************
### location_table ###

Returns the table referenced by `events.location_id`.

******************************************************************************/
CREATE OR REPLACE FUNCTION location_table()
    RETURNS TABLE(location_table TEXT) AS
$$
BEGIN
    RETURN QUERY
        SELECT
            A.location_table
        FROM location_table AS A;
END;
$$  LANGUAGE plpgsql IMMUTABLE
    SECURITY DEFINER
    -- Set a secure search_path: trusted schema(s), then 'pg_temp'.
    SET search_path = public, pg_temp;

/******************************************************************************
### close_all_connections ###

Closes all active connections to the database except the one calling the
function. This function should be used with extreme care.

******************************************************************************/
CREATE OR REPLACE FUNCTION close_all_connections()
    RETURNS TABLE (closed_connections bigint) AS
$BODY$
    SELECT count(pg_terminate_backend(pg_stat_activity.pid))
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = 'flowdb'
            AND pid <> pg_backend_pid()
$BODY$
LANGUAGE SQL;

/******************************************************************************
### median ###

Calculates the median over a column of values. Works as an aggregate function.
Original function can be found at:

	https://wiki.postgresql.org/wiki/Aggregate_Median

******************************************************************************/
CREATE OR REPLACE FUNCTION _final_median(anyarray) 
	RETURNS float8 AS 
$$ 
	WITH q AS
	(
		SELECT val
		FROM unnest($1) val
		WHERE VAL IS NOT NULL
		ORDER BY 1
	),
		cnt AS
	(
		SELECT COUNT(*) AS c FROM q
	)
		SELECT AVG(val)::float8
		FROM 
	(
		SELECT val FROM q
		LIMIT  2 - MOD((SELECT c FROM cnt), 2)
		OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)  
	) q2;
$$ LANGUAGE SQL IMMUTABLE;
 
CREATE AGGREGATE median(anyelement) (
	SFUNC=array_append,
	STYPE=anyarray,
	FINALFUNC=_final_median,
	INITCOND='{}'
);

/******************************************************************************
### isnumeric ###

Boolean test for the type-casting of a TEXT into a NUMERIC type. The original
code comes from this StackOverflow post:

	http://stackoverflow.com/questions/16195986/isnumeric-with-postgresql

******************************************************************************/
CREATE OR REPLACE FUNCTION isnumeric(text) 
	RETURNS BOOLEAN AS 
$$
	DECLARE x NUMERIC;
	BEGIN
		x = $1::NUMERIC;
		RETURN TRUE;
	EXCEPTION WHEN others THEN
		RETURN FALSE;
	END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;

/******************************************************************************
### long_running_queries ###

Utility function to check long running queries. Long-running queries
are those queries running for more than five minutes. 

******************************************************************************/
CREATE OR REPLACE FUNCTION long_running_queries()
    RETURNS TABLE (
		pid INTEGER,
		duration INTERVAL,
		query TEXT,
		state TEXT
	) AS
$$
BEGIN
	RETURN QUERY
		SELECT
			pg_stat_activity.pid,
			now() - pg_stat_activity.query_start AS duration,
			pg_stat_activity.query,
			pg_stat_activity.state
		FROM pg_stat_activity
		WHERE now() - pg_stat_activity.query_start > interval '5 minutes';
END
$$
STRICT LANGUAGE plpgsql IMMUTABLE;

/*********************************
### random_ints ###

Generates a repeatable random sample of distinct integers between 1 and max_val based on
some number of draws.

***********************************/

CREATE OR REPLACE FUNCTION random_ints (seed DOUBLE PRECISION, n_samples INT, max_val INT)
             RETURNS TABLE (id INT)
            AS $$
DECLARE new_seed NUMERIC;
DECLARE samples double precision[] := array[]::double precision[];
BEGIN
 new_seed = random();
 PERFORM setseed(seed);
 FOR i in 1..n_samples LOOP
    samples := array_append(samples, random());
 END LOOP;
 PERFORM setseed(new_seed);
 RETURN QUERY SELECT
    round(samples[generate_series] * max_val)::integer as id
    FROM generate_series(1, n_samples)
    GROUP BY id;
END; $$

LANGUAGE plpgsql
SET search_path = public, pg_temp;
