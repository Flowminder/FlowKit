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

- geoms:       this contains the geometries and their metadata as referenced by the geo bridge.

- geo_bridge:  this is populated with a link between the 
               infrastructure.cells table, and each of the geography
               types.
-----------------------------------------------------------
*/
CREATE SCHEMA IF NOT EXISTS geography;

CREATE TABLE IF NOT EXISTS geography.geoms (
    gid BIGSERIAL PRIMARY KEY,
    added_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    short_name VARCHAR, /* e.g. admin3pcod */
    long_name TEXT, /* e.g. admin3name */
    kind VARCHAR DEFAULT 'admin_unit', /* The type of the geom - admin unit, coverage polygon etc. */
    spatial_resolution INT, /* Admin level, grid spatial resolution */
    additional_metadata JSON /* Catch all field for additional metadata */
);
SELECT AddGeometryColumn('geography','geoms','geom','4326','MULTIPOLYGON',2);
CREATE INDEX "geography_geom_gist" ON "geography"."geoms" USING GIST ("geom");

CREATE TABLE IF NOT EXISTS geography.geo_bridge (
    cell_id BIGINT, /* FK to interactions.locations, constraint added when creating that table */
    geo_id BIGINT REFERENCES  geography.geoms(gid),
    valid_from DATE,
    valid_to DATE,
    weight DOUBLE PRECISION DEFAULT 1, /* Where a cell links to multiple geoms, this may be less than 1 */
    linkage_method TEXT, /* Indicator for how the geometry is linked */
    PRIMARY KEY (cell_id, geo_id, linkage_method)
    );