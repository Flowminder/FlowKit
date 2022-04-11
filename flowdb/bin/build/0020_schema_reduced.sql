/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
REDUCED -------------------------------------------------

This schema contains data from operators that has been cut,
compressed or otherwise reduced into a processing-specific
format.

At present, it contains the 'sightings' table, that sorts
and accumulates CRD by consecutive sightings on the same
tower, and the partitions for same.

----------------------------------------------------------
*/

BEGIN;
CREATE SCHEMA IF NOT EXISTS reduced;
CREATE TABLE IF NOT EXISTS reduced.sightings(
    sighting_date DATE ,  -- Actually partition
    sub_id bytea NOT NULL, -- change to bytea
    sighting_id INTEGER NOT NULL,
    location_id text, --NOT NULL REFERENCES reduced.cell_location_mapping(location_id),  -- change to bytea
    event_times TIME[],
    event_types smallint[] -- change to enum
) PARTITION BY RANGE (sighting_date);
CREATE INDEX IF NOT EXISTS sighting_date_ind ON reduced.sightings(sighting_date);
COMMIT;