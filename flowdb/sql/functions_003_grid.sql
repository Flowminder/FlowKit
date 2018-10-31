/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*

    makegrid_2d
    -----------

    http://gis.stackexchange.com/questions/16374/how-to-create-a-regular-polygon-grid-in-postgis

*/

CREATE OR REPLACE FUNCTION public.makegrid_2d (
      bound_polygon public.geometry,
      grid_step numeric,
      metric_srid integer = 3857 --metric SRID (web mercator)
    )
    RETURNS public.geometry AS
    $body$
    DECLARE
      BoundM public.geometry; --Bound polygon transformed to the metric projection (with metric_srid SRID)
      Xmin DOUBLE PRECISION;
      Xmax DOUBLE PRECISION;
      Ymax DOUBLE PRECISION;
      X DOUBLE PRECISION;
      Y DOUBLE PRECISION;
      sectors public.geometry[];
      i INTEGER;
    BEGIN
      BoundM := ST_Transform($1, $3); --From WGS84 (SRID 4326) to the metric projection, to operate with step in meters
      Xmin := ST_XMin(BoundM);
      Xmax := ST_XMax(BoundM);
      Ymax := ST_YMax(BoundM);

      Y := ST_YMin(BoundM); --current sector's corner coordinate
      i := -1;
      <<yloop>>
      LOOP
        IF (Y > Ymax) THEN  --Better if generating polygons exceeds the bound for one step. You always can crop the result. But if not you may get not quite correct data for outbound polygons (e.g. if you calculate frequency per sector)
            EXIT;
        END IF;

        X := Xmin;
        <<xloop>>
        LOOP
          IF (X > Xmax) THEN
              EXIT;
          END IF;

          i := i + 1;
          sectors[i] := ST_GeomFromText('POLYGON(('||X||' '||Y||', '||(X+$2)||' '||Y||', '||(X+$2)||' '||(Y+$2)||', '||X||' '||(Y+$2)||', '||X||' '||Y||'))', $3);

          X := X + $2;
        END LOOP xloop;
        Y := Y + $2;
      END LOOP yloop;

      RETURN ST_Transform(ST_Collect(sectors), ST_SRID($1));
    END;
    $body$
    LANGUAGE 'plpgsql' STABLE PARALLEL SAFE;