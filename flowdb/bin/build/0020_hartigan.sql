/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
    Hartigan
    --------

    Hartigan clustering algorithm function.

*/


-- drop functions if they already exists
DROP TYPE IF EXISTS hartigan_cluster_interm CASCADE;
DROP TYPE IF EXISTS hartigan_cluster CASCADE;
-- create a new data type to hold the final results
-- each object of this type is described further below
CREATE TYPE hartigan_cluster_interm AS (clusters       geography[],
                                        rank           integer[],
                                        weights        integer[],
                                        site_ids   text[],
                                        versions       text[],
                                        buffer         double precision,
                                        call_threshold integer);

CREATE TYPE hartigan_cluster AS (clusters     geography[],
                                 rank         integer[],
                                 weights      integer[],
                                 site_ids text[],
                                 versions     text[]);

-- function containing the Hartigan clustering logic
CREATE OR REPLACE FUNCTION hartigan_assign(har              hartigan_cluster_interm,
                                           site_id      text,
                                           version          integer,
                                           weight           integer,
                                           threshold        double precision,
                                           buffer           double precision,
                                           call_threshold   integer)
RETURNS hartigan_cluster_interm AS $$

DECLARE
    i integer;
    -- an array which contains all the calculated clusters
    clusters geography[] := har.clusters;
    -- an array which contains the rank of a given cluster
    rank integer[] := har.rank;
    -- an array which contains the total weights of each cluster (eg. sum of call days)
    weights integer[] := har.weights;
    -- an array which contains all the the site_ids and versions which form a cluster
    site_ids text[] := har.site_ids;
    versions text[] := har.versions;
    -- the threshold used to cluster new points
    threshold double precision := threshold * 1000;
    -- temporary variables used for calculation
    new_weight integer;
    lon double precision;
    lat double precision;
    -- gets the coordinate of the current site id
    coord geography:= geom_point FROM infrastructure.sites sites WHERE sites.id=$2 AND sites.version=$3;

BEGIN
    -- loop through all available clusters
    -- it uses COALESCE in case the `clusters` array is empty as we need to loop at least once
    FOR i IN 1..array_length(COALESCE(clusters, ARRAY[ST_MakePoint(0,0)]), 1) LOOP
        -- check if the current coordinate is within the given cluster threshold
        IF ST_DISTANCE(clusters[i], coord) < threshold THEN
            -- calculates new weights and coordinates
            new_weight = weights[i] + weight;
            lon = ((ST_X(clusters[i]::geometry)*weights[i]) + (ST_X(coord::geometry)*weight))/(new_weight);
            lat = ((ST_Y(clusters[i]::geometry)*weights[i]) + (ST_Y(coord::geometry)*weight))/(new_weight);
            clusters[i] = ST_SetSRID(ST_MakePoint(lon, lat), '4326');
            weights[i] = new_weight;
            site_ids[i] = site_ids[i]||'::'||site_id;
            versions[i] = versions[i]||'::'||version::text;
            -- exit the loop since we found a matching point for the coordinate
            EXIT;
        -- if no matching cluster is found for the coordinate we start a new one
        -- we do that when we reached the end of the `clusters` list or when
        -- the `clusters` array is null which means we just started the algorithm
        ELSIF i = array_upper(clusters, 1) OR clusters IS NULL THEN
            IF clusters IS NULL THEN
                rank = array_append(rank, i);
            ELSE
                rank = array_append(rank, i+1);
            END IF;
                clusters = array_append(clusters, coord);
                weights = array_append(weights, weight);
                site_ids = array_append(site_ids, site_id);
                versions = array_append(versions, version::text);
        END IF;
    END LOOP;

    RETURN ROW(clusters, rank, weights, site_ids, versions, buffer, call_threshold);

END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;

-- function containing the buffering logic
CREATE OR REPLACE FUNCTION hartigan_buffer(hartigan_cluster_interm)
RETURNS hartigan_cluster AS $$
DECLARE
    i integer;
    -- an array which contains all the calculated clusters
    clusters geography[];
    new_cluster geography;
    -- an array which contains the rank of a given cluster
    rank integer[];
    -- an array which contains the total weights of each cluster (eg. sum of call days)
    weights integer[];
    -- an array which contains all the the site_ids and versions which form a cluster
    site_ids text[];
    versions text[];
BEGIN
    FOR i in 1..array_length($1.clusters, 1) LOOP
        -- filter clusters by minimum call day threshold
        IF $1.weights[i] >= $1.call_threshold THEN
            rank = array_append(rank, $1.rank[i]);
            weights = array_append(weights, $1.weights[i]);
            site_ids = array_append(site_ids, $1.site_ids[i]);
            versions = array_append(versions, $1.versions[i]);
            -- only buffers if buffer is more than 0 otherwise do nothing
            IF $1.buffer > 0 THEN
                -- if the symbol '::' is present in the version string, it means
                -- that the cluster is formed by more than one site
                IF $1.versions[i] ~ '::' THEN
                    new_cluster = ST_Buffer($1.clusters[i], $1.buffer * 1000);
                    clusters = array_append(clusters, new_cluster);
                -- otherwise just take the associated polygon associated with the site
                ELSE
                    new_cluster =  geom_polygon FROM infrastructure.sites sites
                                                WHERE sites.id=$1.site_ids[i] AND
                                                      sites.version=$1.versions[i]::integer;
                    IF new_cluster IS NULL THEN
                        new_cluster = ST_Buffer($1.clusters[i], $1.buffer * 1000);
                    END IF;
                    clusters = array_append(clusters, new_cluster);
                END IF;
            ELSE
                clusters = array_append(clusters, $1.clusters[i]);
            END IF;
        END IF;
    END LOOP;
    RETURN ROW(clusters, rank, weights, site_ids, versions);
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;

-- creates an aggregate function which loops through each element of the arguments
-- and keeps the state of the algorithm in a variable called `hartigan_cluster` which
-- is eventually returned when all the elements are processed.
-- in order for the algorithm to work, the aggregate function should receive the arguments
-- sorted by the `weight`
CREATE AGGREGATE hartigan (site_id text,
                           version integer,
                           weight integer,
                           threshold double precision,
                           buffer double precision,
                           call_threshold integer)
(
    sfunc = hartigan_assign,
    stype = hartigan_cluster_interm,
    finalfunc = hartigan_buffer
);