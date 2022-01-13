CREATE TABLE IF NOT EXISTS reduced.sightings(
    sighting_date DATE,
    msisdn bytea NOT NULL, -- change to bytea
    sighting_id INTEGER NOT NULL,
    location_id text, --NOT NULL REFERENCES reduced.cell_location_mapping(location_id),  -- change to bytea
    event_times TIME[],
    event_types smallint[]
);