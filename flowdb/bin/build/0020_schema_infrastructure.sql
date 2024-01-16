/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
INFRASTRUCTURE -----------------------------------------

This schema represents the telecommunications
infrastructure, that is, the physical things
that make a mobile network work. This schema also
includes tables data about TACs.

Available tables are:

  - sites:      with sites information.
  - cells:      with cell information.
  - tacs:       with the TAC codes for devices.

--------------------------------------------------------
*/
CREATE SCHEMA IF NOT EXISTS infrastructure;

    CREATE TABLE IF NOT EXISTS infrastructure.sites(
        site_id BIGSERIAL PRIMARY KEY,
        id TEXT,
        version INTEGER,

        name TEXT,
        type TEXT,

        status TEXT,
        structure_type TEXT,

        is_cow BOOLEAN,

        date_of_first_service DATE,
        date_of_last_service DATE,

        UNIQUE (id, version)

        );
    
    /*

        Here we use PostGIS' AddGeometryColumn
        for creating the geometry columns `geom_point`
        and `geom_polygon` on the infrasturcture.*
        columns. That function has advantages because
        it registers the SRID of columns with PostGIS
        metadata and creates a typmod geometry column
        for checking the SRID and the column specific
        geometry type (e.g `POINT`, `MULTIPOLYGON`).
        The same operations is repeated in the 
        infrastructure.sites column.

    */
    SELECT AddGeometryColumn('infrastructure', 'sites', 'geom_point', 4326, 'POINT', 2);
    SELECT AddGeometryColumn('infrastructure', 'sites', 'geom_polygon', 4326, 'MULTIPOLYGON', 2);

    CREATE INDEX IF NOT EXISTS infrastructure_sites_geom_point_index
        ON infrastructure.sites
        USING GIST (geom_point);

    CREATE INDEX IF NOT EXISTS infrastructure_sites_geom_polygon_index
        ON infrastructure.sites
        USING GIST (geom_polygon);

    CREATE INDEX ON infrastructure.sites (id);

    CREATE TABLE IF NOT EXISTS infrastructure.cells(
        cell_id BIGSERIAL PRIMARY KEY,
        id TEXT,
        version INTEGER,
        site_id TEXT,

        name TEXT,
        type TEXT,

        msc TEXT,
        bsc_rnc TEXT,
        
        antenna_type TEXT,
        status TEXT,

        lac TEXT,
        height NUMERIC,
        azimuth NUMERIC,
        transmitter TEXT,
        max_range NUMERIC,
        min_range NUMERIC,
        electrical_tilt NUMERIC,
        mechanical_downtilt NUMERIC,

        date_of_first_service DATE,
        date_of_last_service DATE,
        UNIQUE (id, version)
        );

    SELECT AddGeometryColumn('infrastructure', 'cells', 'geom_point', 4326, 'POINT', 2);
    SELECT AddGeometryColumn('infrastructure', 'cells', 'geom_polygon', 4326, 'MULTIPOLYGON', 2);

    CREATE INDEX IF NOT EXISTS infrastructure_cells_geom_point_index
        ON infrastructure.cells
        USING GIST (geom_point);

    CREATE INDEX IF NOT EXISTS infrastructure_cells_geom_polygon_index
        ON infrastructure.cells
        USING GIST (geom_polygon);

    CREATE INDEX ON infrastructure.cells (id);
    CREATE INDEX ON infrastructure.cells (site_id);
    
    CREATE TABLE IF NOT EXISTS infrastructure.tacs(

        id NUMERIC PRIMARY KEY,
        brand TEXT,
        model TEXT,
        width NUMERIC,
        height NUMERIC,
        depth NUMERIC,
        weight NUMERIC,
        display_type TEXT,
        display_colors BOOLEAN,
        display_width NUMERIC,
        display_height NUMERIC,
        mms_receiver BOOLEAN,
        mms_built_in_camera BOOLEAN,
        wap_push_ota_support BOOLEAN,
        hardware_gprs BOOLEAN,
        hardware_edge BOOLEAN,
        hardware_umts BOOLEAN,
        hardware_wifi BOOLEAN,
        hardware_bluetooth BOOLEAN,
        hardware_gps BOOLEAN,
        software_os_vendor TEXT,
        software_os_name TEXT,
        software_os_version TEXT,
        wap_push_ota_settings BOOLEAN,
        wap_push_ota_bookmarks BOOLEAN,
        wap_push_ota_app_internet BOOLEAN,
        wap_push_ota_app_browser BOOLEAN,
        wap_push_ota_app_mms BOOLEAN,
        wap_push_ota_single_shot BOOLEAN,
        wap_push_ota_multi_shot BOOLEAN,
        wap_push_oma_settings BOOLEAN,
        wap_push_oma_app_internet BOOLEAN,
        wap_push_oma_app_browser BOOLEAN,
        wap_push_oma_app_mms BOOLEAN,
        wap_push_oma_cp_bookmarks BOOLEAN,
        wap_1_2_1 BOOLEAN,
        wap_2_0 BOOLEAN,
        syncml_dm_settings BOOLEAN,
        syncml_dm_acc_gprs BOOLEAN,
        syncml_dm_app_internet BOOLEAN,
        syncml_dm_app_browser BOOLEAN,
        syncml_dm_app_mms BOOLEAN,
        syncml_dm_app_bookmark BOOLEAN,
        syncml_dm_app_java BOOLEAN,
        wap_push_oma_app_ims BOOLEAN,
        wap_push_oma_app_poc BOOLEAN,
        j2me_midp_10 BOOLEAN,
        j2me_midp_20 BOOLEAN,
        j2me_midp_21 BOOLEAN,
        j2me_cldc_10 BOOLEAN,
        j2me_cldc_11 BOOLEAN,
        j2me_cldc_20 BOOLEAN,
        hnd_type TEXT

        );

    -- Table to record each time the cell info is updated
    CREATE TABLE IF NOT EXISTS infrastructure.cells_table_versions (
        id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        source_filename TEXT,
        file_hash TEXT,
        file_date DATE,
    );

    -- Table to keep records of all cells (including those excluded due to data quality issues, and old versions of cells that have moved/changed).
    -- Keeping all cell info in this table allows us to reconstruct the cells table as it was at a previous point in time, if necessary.
    CREATE TABLE IF NOT EXISTS infrastructure.cell_info(
        id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        cells_table_version INTEGER NOT NULL REFERENCES infrastructure.cells_table_versions (id),
        mno_cell_id TEXT NOT NULL, -- 'id' in infrastructure.cells
        dates_of_service TSTZRANGE NOT NULL, -- [date_of_first_service,date_of_last_service) in infrastructure.cells
        to_include BOOLEAN NOT NULL, -- Flag to indicate whether or not cell should be used in analysis
        longitude DOUBLE PRECISION, -- x component of 'geom_point' in infrastructure.cells
        latitude DOUBLE PRECISION, -- y component of 'geom_point' in infrastructure.cells
        geom_point geometry(Point, 4326) GENERATED ALWAYS AS ( ST_SetSRID(ST_Point(longitude, latitude), 4326) ) STORED,
        technology TEXT, -- 'type' in infrastructure.cells
        cell_name TEXT, -- 'name' in infrastructure.cells
        mno_site_id TEXT, -- 'site_id' in infrastructure.cells
        msc TEXT,
        bsc_rnc TEXT,
        antenna_type TEXT,
        status TEXT,
        lac TEXT,
        height NUMERIC,
        azimuth NUMERIC,
        transmitter TEXT,
        max_range NUMERIC,
        min_range NUMERIC,
        electrical_tilt NUMERIC,
        mechanical_downtilt NUMERIC,
        included_in_latest_file BOOLEAN NOT NULL, -- True if cell was included in latest cell info file; false if it was copied over from previous cells table and not in latest file
        additional_metadata JSONB, -- JSON field to catch any fields provided in cell info files that don't fit into the infrastructure.cells table structure
        notes TEXT, -- Free text field for adding notes related to this cell (e.g. reason for exclusion)
        EXCLUDE USING GIST (cells_table_version WITH =, mno_cell_id WITH =, dates_of_service WITH &&) WHERE (to_include) -- ensure cell ID is unique across simultaneously-valid cells (so a CDR event can never map to multiple cells)
            -- Note: this exclude constraint requires btree_gist extension (https://dba.stackexchange.com/questions/37351/postgresql-exclude-using-error-data-type-integer-has-no-default-operator-class)
    );
