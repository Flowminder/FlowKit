BEGIN;

-- Partitions are inclusive on lower, exclusive on upper
ALTER TABLE reduced.sightings
ATTACH PARTITION reduced.sightings_{{params.date}} FOR VALUES FROM (date('{{params.date}}')) TO (date('{{params.date}}')+ 1) ;

ALTER TABLE reduced.sightings_{{params.date}}
DROP CONSTRAINT sightings_{{params.date}}_check;

ANALYZE reduced.sightings;

COMMIT;