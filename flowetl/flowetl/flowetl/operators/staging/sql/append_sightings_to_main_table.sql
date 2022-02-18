BEGIN;

-- Partitions are inclusive on lower, exclusive on upper
ALTER TABLE reduced.sightings
ATTACH PARTITION reduced.sightings_{{ds_nodash}} FOR VALUES FROM (date('{{ds_nodash}}')) TO (date('{{ds_nodash}}')+ 1) ;

ALTER TABLE reduced.sightings_{{ds_nodash}}
DROP CONSTRAINT sightings_{{ds_nodash}}_check;

ANALYZE reduced.sightings;

COMMIT;