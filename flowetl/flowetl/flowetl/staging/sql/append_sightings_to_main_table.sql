BEGIN;

ALTER TABLE reduced.sightings_{{date}} ADD CONSTRAINT {{date}}
    CHECK (sighting_date == DATE '{{date}}')

-- Partitions are inclusive on the lower bound, exclusive on the upper
ALTER TABLE reduced.sightings ATTACH PARTITION reduced.sightings_{{date}}
    FOR VALUES FROM (DATE '{{date}}') TO (DATE '{{date}}') + 1
