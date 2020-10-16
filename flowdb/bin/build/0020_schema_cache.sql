/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
    Cache schema
    ------------
    Schema and tables for managing caching of queries.

*/

CREATE SCHEMA IF NOT EXISTS cache;
CREATE TABLE IF NOT EXISTS cache.cached
                            (
                                query_id CHARACTER(32) NOT NULL,
                                version CHARACTER VARYING,
                                query TEXT,
                                created TIMESTAMP WITH TIME ZONE,
                                access_count INTEGER,
                                last_accessed TIMESTAMP WITH TIME ZONE,
                                compute_time NUMERIC,
                                cache_score_multiplier NUMERIC,
                                class CHARACTER VARYING,
                                schema CHARACTER VARYING,
                                tablename CHARACTER VARYING,
                                obj BYTEA,
                                CONSTRAINT cache_pkey PRIMARY KEY (query_id)
                            );
/* Sequence counting total number of retrievals from cache */
CREATE SEQUENCE cache.cache_touches START 1;
CREATE TABLE IF NOT EXISTS cache.dependencies
                            (
                                query_id CHARACTER(32) NOT NULL,
                                depends_on CHARACTER(32) NOT NULL,
                                CONSTRAINT dependencies_pkey PRIMARY KEY (depends_on, query_id),
                                CONSTRAINT cache_dependency_id FOREIGN KEY (depends_on)
                                    REFERENCES cache.cached (query_id) MATCH SIMPLE
                                    ON UPDATE NO ACTION
                                    ON DELETE CASCADE,
                                CONSTRAINT cache_dependent_id FOREIGN KEY (query_id)
                                    REFERENCES cache.cached (query_id) MATCH SIMPLE
                                    ON UPDATE NO ACTION
                                    ON DELETE CASCADE
                            );

CREATE TABLE cache.cache_config (key text, value text);
INSERT INTO cache.cache_config (key, value) VALUES ('half_life', NULL);
INSERT INTO cache.cache_config (key, value) VALUES ('cache_size', NULL);
INSERT INTO cache.cache_config (key, value) VALUES ('cache_protected_period', NULL);

CREATE TABLE cache.zero_cache (object_class text);
INSERT INTO cache.zero_cache (object_class) VALUES ('Table'), ('GeoTable'), ('CallsTable'), ('SmsTable'), ('MdsTable'), ('TopupsTable'), ('ForwardsTable'), ('TacsTable'), ('CellsTable'), ('SitesTable');