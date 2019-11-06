/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
GEOGRAPHY -------------------------------------------------

This schema is a placeholder for all tables created
automatically by shapefile and raster file transformation
scripts.

- geo_bridge:  this is populated with a link between the 
               infrastructure.cells table, and each of the geography
               types.
-----------------------------------------------------------
*/
CREATE SCHEMA IF NOT EXISTS geography;

    CREATE TABLE IF NOT EXISTS geography.geo_bridge (

        cell_id BIGINT,
        geo_id NUMERIC,
        valid_from DATE,
        valid_to DATE,
        linkage_method TEXT,
        PRIMARY KEY (cell_id, geo_id, linkage_method)

        );
