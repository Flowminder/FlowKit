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
that make a mobile network work. This schem also
includes tables data about countries, operators, 
and TACs.

Available tables are:

  - countries:  with the country codes in ISO format.
  - operators:  with information about telecom 
                operators from around the world.
  - sites:      with sites information.
  - cells:      with cell information.
  - tacs:       with the TAC codes for devices.

--------------------------------------------------------
*/
CREATE SCHEMA IF NOT EXISTS infrastructure;

    CREATE TABLE IF NOT EXISTS infrastructure.countries(

        id NUMERIC,
        name NUMERIC,
        iso VARCHAR(3)

        );

    CREATE TABLE IF NOT EXISTS infrastructure.operators(

        id NUMERIC,
        name TEXT,
        country TEXT,
        iso VARCHAR(3)

        );

    CREATE TABLE IF NOT EXISTS infrastructure.sites(

        id TEXT,
        version INTEGER,

        name TEXT,
        type TEXT,

        status TEXT,
        structure_type TEXT,

        is_cow BOOLEAN,

        date_of_first_service DATE,
        date_of_last_service DATE,

        PRIMARY KEY (id, version)

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

        PRIMARY KEY(id, version)

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
