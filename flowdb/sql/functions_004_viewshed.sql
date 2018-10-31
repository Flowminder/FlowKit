/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
    Viewshed functions
    ------------------

    Functions for running viewshed modules.
*/

-- Count the number of distinct sightlines in a table.

CREATE OR REPLACE FUNCTION _viewshed_count_total(_tbl regclass, OUT result integer)
            AS $func$
            BEGIN
                EXECUTE format('SELECT count(*)
                                FROM (SELECT distinct S.od_line_identifier
                                FROM %s AS S) AS S', _tbl)
                INTO result;
            END;
        $func$ LANGUAGE plpgsql STABLE PARALLEL SAFE;

-- Check which geoms are visible on this sightline, and which are occluded.
-- Iterate through the geoms by distance from the ray caster, and let the terrain height
-- be the highest point encountered. Terrain _lower_ than that height and further away
-- is occluded.

CREATE OR REPLACE FUNCTION _visible (_tbl regclass, line_of_sight_identifier TEXT)
    RETURNS TABLE (
        location_id TEXT,
        geom_point GEOMETRY,
        od_line_identifier TEXT,
        geom GEOMETRY,
        visible BOOLEAN
    ) AS $func$

    DECLARE
        mu FLOAT8 := '-infinity';
        distance NUMERIC;
        slope NUMERIC;

    BEGIN

        FOR location_id, geom_point, od_line_identifier, geom, distance, slope IN
        EXECUTE
            format(
                '
                SELECT
                    S.location_id,
                    S.geom_point,
                    S.od_line_identifier,
                    S.geom,
                    S.distance,
                    S.slope
                FROM %s AS S
                WHERE S.od_line_identifier::uuid = ''%s''::uuid
                ', _tbl, line_of_sight_identifier
            )
        LOOP
            visible := (slope > mu);
            RETURN NEXT;
            IF (slope > mu) THEN
                mu := slope;
            END IF;
        END LOOP;

    END;
$func$ LANGUAGE plpgsql STABLE PARALLEL SAFE;

-- For use in combination with FlowMachine's _viewshedSlopes method
-- For table of target geoms, and sightlines from towers, checks for
-- each geom whether that geom is 'visible' along the sightline, or occluded.
-- This is based on a model of the elevation of the area.
-- Essentially, a raycaster for cell tower coverage.

CREATE OR REPLACE FUNCTION viewshed (_tbl regclass)

    RETURNS TABLE (
        location_id TEXT,
        geom_point GEOMETRY,
        od_line_identifier TEXT,
        geom_viewshed GEOMETRY,
        visible BOOLEAN
    ) AS $func$

    DECLARE
        i NUMERIC := 0;
        pb NUMERIC;
        total NUMERIC;

    BEGIN

        total := (
            SELECT _viewshed_count_total(_tbl)
        );

        FOR od_line_identifier IN
        EXECUTE format('
            SELECT
                distinct(S.od_line_identifier)
            FROM %s AS S
        ', _tbl
        )
        LOOP
            pb := (i / total);
            RAISE NOTICE 'Processing LoS line: %', round(pb, 2);
            RETURN QUERY SELECT * FROM _visible(_tbl, od_line_identifier);
            i := i + 1;
        END LOOP;

    END;
$func$ LANGUAGE plpgsql STABLE PARALLEL SAFE;