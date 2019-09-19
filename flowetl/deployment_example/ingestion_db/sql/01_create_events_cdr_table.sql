/*
EVENTS ---------------------------------------------------
This schema collection organizes data provided by operators
into a predictable format.

-----------------------------------------------------------
*/
create extension if not exists timescaledb cascade;

CREATE SCHEMA IF NOT EXISTS events;

CREATE SCHEMA IF NOT EXISTS reference;

CREATE TABLE IF NOT EXISTS events.cdr (
        event_time          timestamptz not null,
        msisdn              text not null,
        cell_id             text not null
);

/*
Typically recommend setting the interval so that these chunk(s) comprise no more
than 25% of main memory.
If you are writing 10GB per day and have 64GB of memory, setting the time interval
to a day would be appropriate.
If you are writing 190GB per day and have 128GB of memory, setting the time interval
to 4 hours would be appropriate.
*/
SELECT create_hypertable('events.cdr', 'event_time', chunk_time_interval => interval '1 day', associated_schema_name => 'events');