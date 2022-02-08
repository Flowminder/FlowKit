BEGIN;

ALTER TABLE reduced.sightings_{{params.date}} ADD CONSTRAINT '{{params.date}}'
    CHECK (sighting_date == DATE '{{params.date}}')

-- Partitions are inclusive on the lower bound, exclusive on the upper
ALTER TABLE reduced.sightings ATTACH PARTITION reduced.sightings_{{params.date}}
    FOR VALUES FROM (DATE '{{params.date}}') TO (DATE '{{params.date}}') + 1
