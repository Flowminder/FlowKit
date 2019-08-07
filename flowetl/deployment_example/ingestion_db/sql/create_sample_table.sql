create table if not exists events.sample (
       event_time          timestamptz not null,
       msisdn              varchar(64),
       cell_id             varchar(20)
);

select create_hypertable('events.sample', 'event_time', chunk_time_interval => interval '1 day');
