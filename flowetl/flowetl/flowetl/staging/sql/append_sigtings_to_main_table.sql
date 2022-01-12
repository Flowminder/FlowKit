BEGIN;

CREATE TABLE IF NOT EXISTS reduced.sightings;
    sighting_date DATE,
    msisdn bytea NOT NULL, -- change to bytea
    sighting_id INTEGER NOT NULL,
    location_id text, --NOT NULL REFERENCES reduced.cell_location_mapping(location_id),  -- change to bytea
    event_times TIME[],
    event_types smallint[]
)
PARTITION BY sighting_date;

ALTER TABLE reduced.sightings_{date} ADD CONSTRAINT {date}
    CHECK (sighting_date == DATE '{date}')

ALTER TABLE measurement_y2008m02 ADD CONSTRAINT y2008m02
   CHECK ( logdate >= DATE '2008-02-01' AND logdate < DATE '2008-03-01' );

ALTER TABLE reduced.sightings ATTACH PARTITION reduced.sightings_{date}
    FOR VALUE (DATE '{date}')
