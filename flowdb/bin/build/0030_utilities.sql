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
BEGIN
 new_seed = random();
 PERFORM setseed(seed);
 RETURN QUERY SELECT generate_series AS id FROM generate_series(1, max_val) ORDER BY random() LIMIT n_samples;
 PERFORM setseed(new_seed);
END; $$

LANGUAGE plpgsql
SET search_path = public, pg_temp;

/*
cache_half_life

Returns the current setting for cache half-life, which governs how much priority is
given to recency of access.
 */

CREATE OR REPLACE FUNCTION cache_half_life()
	RETURNS float AS
$$
  DECLARE halflife float;
  BEGIN
  SELECT value INTO halflife FROM cache.cache_config WHERE key='half_life';
  RETURN halflife;
  END
$$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp;


/*
cache_max_size

Returns the current setting for cache size in bytes.
 */

CREATE OR REPLACE FUNCTION cache_max_size()
	RETURNS bigint AS
$$
  DECLARE cache_size bigint;
  BEGIN
  SELECT value INTO cache_size FROM cache.cache_config WHERE key='cache_size';
  RETURN cache_size;
  END
$$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp;

/*********************************
### table_size ###

Get the size on disk in bytes of a table in the database.

***********************************/

CREATE OR REPLACE FUNCTION table_size(IN tablename TEXT, IN table_schema TEXT)
	RETURNS float AS
$$
  DECLARE table_size bigint;
  BEGIN
  SELECT pg_total_relation_size(c.oid) INTO table_size
              FROM pg_class c
              LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE relkind = 'r' AND relname=tablename AND nspname=table_schema;
  RETURN table_size;
  END
$$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp;

/*********************************
### touch_cache ###

Update the cache score, access count and most recent access time of a cached query and return
the new cache score, or raise an error if no cached query with that id exists.

***********************************/

CREATE OR REPLACE FUNCTION touch_cache(IN cached_query_id TEXT)
	RETURNS float AS
$$
  DECLARE score float;
  BEGIN
  UPDATE cache.cached SET last_accessed = NOW(), access_count = access_count + 1,
        cache_score_multiplier = CASE WHEN class='Table' THEN 0 ELSE
          cache_score_multiplier+POWER(1 + ln(2) / cache_half_life(), nextval('cache.cache_touches') - 2)
        END
        WHERE query_id=cached_query_id
        RETURNING cache_score(cache_score_multiplier, compute_time, greatest(table_size(tablename, schema), 0.00001)) INTO score;
        IF NOT FOUND THEN RAISE EXCEPTION 'Cache record % not found', cached_query_id;
        END IF;
  RETURN score;
  END
$$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp;

/*********************************
### cache_score ###

Calculate a cache score from a multiplier, compute time in ms and the size of the table.

***********************************/

CREATE OR REPLACE FUNCTION cache_score(IN cache_score_multiplier numeric, IN compute_time numeric, IN tablesize double precision)
	RETURNS float AS
$$
  BEGIN
  RETURN cache_score_multiplier*((compute_time/1000)/tablesize);
  END
$$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp;

/*
cache_protected_period

Returns the current setting for cache protected period as an integer number of seconds.
 */

CREATE OR REPLACE FUNCTION cache_protected_period()
	RETURNS bigint AS
$$
  DECLARE cache_protected_period bigint;
  BEGIN
  SELECT value INTO cache_protected_period FROM cache.cache_config WHERE key='cache_protected_period';
  RETURN cache_protected_period;
  END
$$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp;

/*

Poisson event generator.

*/

CREATE OR REPLACE FUNCTION random_poisson(
    lambda DOUBLE PRECISION DEFAULT 1.0
    ) RETURNS DOUBLE PRECISION
      RETURNS NULL ON NULL INPUT AS $$
        DECLARE
            u DOUBLE PRECISION;
            x DOUBLE PRECISION;
            s NUMERIC;
            p NUMERIC;
        BEGIN
            u = RANDOM();
            p = exp(-lambda);
            x = 0;
            s = p;
            WHILE u > s LOOP
                x = x + 1;
                p = p*lambda/x;
                s = s + p;
            END LOOP;
            RETURN x;
        END;
    $$ LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp;

/* Random pick from array */

CREATE OR REPLACE FUNCTION random_pick( a anyarray, OUT x anyelement )
  RETURNS anyelement AS
$func$
BEGIN
  IF a = '{}' THEN
    x := NULL::TEXT;
  ELSE
    WHILE x IS NULL LOOP
      x := a[floor(array_lower(a, 1) + (random()*( array_upper(a, 1) -  array_lower(a, 1)+1) ) )::int];
    END LOOP;
  END IF;
END
$func$ LANGUAGE plpgsql VOLATILE RETURNS NULL ON NULL INPUT;