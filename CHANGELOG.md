# Changelog
All notable changes to FlowKit will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

## [0.6.1]
### Fixed
- Fixed `TotalNetworkObjects` raising an error when run with a lat-long level [#108](https://github.com/Flowminder/FlowKit/issues/108)
- Radius of gyration no longer incorrectly appears as a top level api query

## [0.6.0]
### Added
- Added new flowclient API entrypoint, `aggregate_network_objects`, to access equivalent flowmachine query [#601](https://github.com/Flowminder/FlowKit/issues/601)
- FlowAPI now exposes the API spec at the `spec/openapi.json` endpoint, and an interactive version of the spec at the `spec/redoc` endpoint
- Added Makefile target `make up-no_build`, to spin up all containers without building the images
- Added `resync_redis_with_cache` function to cache utils, to allow administrators to align redis with FlowDB [#636](https://github.com/Flowminder/FlowKit/issues/636)
- Added new flowclient API entrypoint, `radius_of_gyration`, to access (with simplified parameters) equivalent flowmachine query `RadiusOfGyration` [#602](https://github.com/Flowminder/FlowKit/issues/602)

### Changed
- The `period` argument to `TotalNetworkObjects` in FlowMachine has been renamed `total_by`
- The `period` argument to `total_network_objects` in FlowClient has been renamed `total_by`
- The `by` argument to `AggregateNetworkObjects` in FlowMachine has been renamed to `aggregate_by`
- The `stop_date` argument to the `modal_location_from_dates` and `meaningful_locations_*` functions in FlowClient has been renamed `end_date` [#470](https://github.com/Flowminder/FlowKit/issues/470) 
- `get_result_by_query_id` now accepts a `poll_interval` argument, which allows polling frequency to be changed
- The `start` and `stop` argument to `EventTableSubset` are now mandatory.
- `RadiusOfGyration` now returns a `value` column instead of an `rog` column
- `TotalNetworkObjects` and `AggregateNetworkObjects` now return a `value` column, rather than `statistic_name`
- All environment variables are now in a single `development_environment` file in the project root, development environment setup has been simplified
- Default FlowDB users for FlowMachine and FlowAPI have changed from "analyst" and "reporter" to "flowmachine" and "flowapi", respectively
- Docs and integration tests now use top level compose file
- The following environment variables have been renamed:
  - `FLOWMACHINE_SERVER` (FlowAPI) -> `FLOWMACHINE_HOST`
  - `FM_PASSWORD` (FlowDB), `FLOWDB_PASS` (FlowMachine) -> `FLOWMACHINE_FLOWDB_PASSWORD`
  - `API_PASSWORD` (FlowDB), `FLOWDB_PASS` (FlowAPI) -> `FLOWAPI_FLOWDB_PASSWORD`
  - `FM_USER` (FlowDB), `FLOWDB_USER` (FlowMachine) -> `FLOWMACHINE_FLOWDB_USER`
  - `API_USER` (FlowDB), `FLOWDB_USER` (FlowAPI) -> `FLOWAPI_FLOWDB_USER`
  - `LOG_LEVEL` (FlowMachine) -> `FLOWMACHINE_LOG_LEVEL`
  - `LOG_LEVEL` (FlowAPI) -> `FLOWAPI_LOG_LEVEL`
  - `DEBUG` (FlowDB) -> `FLOWDB_DEBUG`
  - `DEBUG` (FlowMachine) -> `FLOWMACHINE_SERVER_DEBUG_MODE`
- The following Docker secrets have been renamed:
  - `FLOWAPI_DB_USER` -> `FLOWAPI_FLOWDB_USER`
  - `FLOWAPI_DB_PASS` -> `FLOWAPI_FLOWDB_PASSWORD`
  - `FLOWMACHINE_DB_USER` -> `FLOWMACHINE_FLOWDB_USER`
  - `FLOWMACHINE_DB_PASS` -> `FLOWMACHINE_FLOWDB_PASSWORD`
  - `POSTGRES_PASSWORD_FILE` -> `POSTGRES_PASSWORD`
  - `REDIS_PASSWORD_FILE` -> `REDIS_PASSWORD`
- `status` enum in FlowDB renamed to `etl_status`
- `reset_cache` now requires a redis client argument

### Fixed
- Fixed being unable to add new users or servers when running FlowAuth with a Postgres database [#622](https://github.com/Flowminder/FlowKit/issues/622)
- Resetting the cache using `reset_cache` will now reset the state of queries in redis as well [#650](https://github.com/Flowminder/FlowKit/issues/650)
- Fixed `mode` statistic for `AggregateNetworkObjects` [#651](https://github.com/Flowminder/FlowKit/issues/651)

### Removed
- Removed `docker-compose-dev.yml`, and docker-compose files in `docs/`, `flowdb/tests/` and `integration_tests/`.
- Removed `Dockerfile-dev` Dockerfiles
- Removed `ENV` defaults from the FlowMachine Dockerfile
- Removed `POSTGRES_DB` environment variable from FlowDB Dockerfile, database name is now hardcoded as `flowdb`


## [0.5.3]
### Added
- Added new `spatial_aggregate` API endpoint and FlowClient function [#599](https://github.com/Flowminder/FlowKit/issues/599)
- Added new flowclient API entrypoint, total_network_objects(), to access (with simplified parameters) equivalent flowmachine query [#581](https://github.com/Flowminder/FlowKit/issues/581)
- Added new flowclient API entrypoint, location_introversion(), to access (with simplified parameters) equivalent flowmachine query [#577](https://github.com/Flowminder/FlowKit/issues/577)
- Added new flowclient API entrypoint, unique_subscriber_counts(), to access (with simplified parameters) equivalent flowmachine query [#562](https://github.com/Flowminder/FlowKit/issues/562)
- New schema `aggregates` and table `aggregates.aggregates` have been created for maintaining a record of the process and completion of scheduled aggregates.
- New `joined_spatial_aggregate` API endpoint and FlowClient function [#600](https://github.com/Flowminder/FlowKit/issues/600)

### Changed
- `daily_location` and `modal_location` query types are no longer accepted as top-level queries, and must be wrapped using `spatial_aggregate`
- `JoinedSpatialAggregate` no longer accepts positional arguments
- `JoinedSpatialAggregate` now supports "avg", "max", "min", "median", "mode", "stddev" and "variance" stats

### Fixed
- `total_network_objects` no longer returns results from `AggregateNetworkObjects` [#603](https://github.com/Flowminder/FlowKit/issues/603)


## [0.5.2]
### Fixed
- Fixed [#514](https://github.com/Flowminder/FlowKit/issues/514), which would cause the client to hang after submitting a query that couldn't be created
- Fixed [#575](https://github.com/Flowminder/FlowKit/issues/575), so that events at midnight are now considered to be happening on the following day


## [0.5.1]
### Added
- Added `HandsetStats` to FlowMachine.
- Added new `ContactReferenceLocationStats` query class to FlowMachine.
- A new zmq message `get_available_dates` was added to the flowmachine server, along with the `/available_dates`
  endpoint in flowapi and the function `get_available_dates()` in flowclient. These allow to determine the dates
  that are available in the database for the supported event types.

### Changed
- FlowMachine's debugging logs are now from a single logger (`flowmachine.debug`) and include the submodule in the submodule field instead of using it as the logger name
- FlowMachine's query run logger now uses the logger name `flowmachine.query_run_log`
- FlowAPI's access, run and debug loggers are now named `flowapi.access`, `flowapi.query` and `flowapi.debug`
- FlowAPI's access and run loggers, and FlowMachine's query run logger now log to stdout instead of stderr
- Passwords for Redis and FlowDB must now be explicitly provided to flowmachine via argument to `connect`, env var, or secret

### Removed
- FlowMachine and FlowAPI no longer support logging to a file


## [0.5.0]
### Added
- The flowmachine python library is now pip installable (`pip install flowmachine`)
- The flowmachine server now supports additional actions: `get_available_queries`, `get_query_schemas`, `ping`.
- Flowdb now contains a new `dfs` schema and associated tables to process mobile money transactions.
  In addition, `flowdb_testdata` contains sample data for DFS transactions.
- The docs now include three worked examples of CDR analysis using FlowKit.
- Flowmachine now supports calculating the total amount of various DFS metrics (transaction amount,
  commission, fee, discount) per aggregation unit during a given date range. These metrics are also
  exposed in FlowAPI via the query kind `dfs_metric_total_amount`.

### Changed

- The JSON structure when setting queries running via flowapi or the flowmachine server has changed:
  query parameters are now "inlined" alongside the `query_kind` key, rather than nested using a separate `params` key.
  Example:
   - previously: `{"query_kind": "daily_location", "params": {"date": "2016-01-01", "aggregation_unit": "admin3", "method": "last"}}`,
   - now: `{"query_kind": "daily_location", "date": "2016-01-01", "aggregation_unit": "admin3", "method": "last"}`
- The JSON structure of zmq reply messages from the flowmachine server was changed.
  Replies now have the form: `{"status": "[success|error]", "msg": "...", "payload": {...}`.
- The flowmachine server action `get_sql` was renamed to `get_sql_for_query_result`.
- The parameter `daily_location_method` was renamed to `method`.


## [0.4.3]
### Added
- When running integration tests locally, normally pytest will automatically spin up servers for flowmachine and flowapi as part of the test setup.
  This can now be disabled by setting the environment variable `FLOWKIT_INTEGRATION_TESTS_DISABLE_AUTOSTART_SERVERS=TRUE`.
- The integration tests now use the environment variables `FLOWAPI_HOST`, `FLOWAPI_PORT` to determine how to connect to the flowapi server.
- A new data generator has been added to the synthetic data container which supports more data types, simple disaster simulation, and more plausible behaviours as well as increased performance

### Changed
- FlowAPI now reports queued/running status for queries instead of just accepted
- The following environment variables have been renamed:
    - `DB_USER` -> `FLOWDB_USER`
    - `DB_USER` -> `FLOWDB_HOST`
    - `DB_PASS` -> `FLOWDB_PASS`
    - `DB_PW` -> `FLOWDB_PASS`
    - `API_DB_USER` -> `FLOWAPI_DB_USER`
    - `API_DB_PASS` -> `FLOWAPI_DB_PASS`
    - `FM_DB_USER` -> `FLOWMACHINE_DB_USER`
    - `FM_DB_PASS` -> `FLOWMACHINE_DB_PASS`
- Added `numerator_direction` to `ProportionEventType` to allow for proportion of directed events.

### Fixed
- Server no longer loses track of queries under heavy load
- `TopUpBalances` no longer always uses entire topups table

### Removed

- The environment variable `DB_NAME` has been removed.

## [0.4.2]
### Changed
- `MDSVolume` no longer allows specifying the table, and will always use the `mds` table.
- All FlowMachine logs are now in structured json form
- FlowAPI now uses structured logs for debugging messages

## [0.4.1]
### Added
- Added `TopUpAmount`, `TopUpBalance` query classes to FlowMachine.
- Added `PerLocationEventStats`, `PerContactEventStats` to FlowMachine

### Removed
- Removed `TotalSubscriberEvents` from FlowMachine as it is superseded by `EventCount`.

## [0.4.0]
### Added

- Dockerised development setup, with support for live reload of `flowmachine` and `flowapi` after source code changes.
- Pre-commit hook for Python formatting with black.
- Added new `IntereventPeriod`, `ContactReciprocal`, `ProportionContactReciprocal`, `ProportionEventReciprocal`, `ProportionEventType` and `MDSVolume` query classes to FlowMachine.

### Changed
- `CustomQuery` now requires column names to be specified
- Query classes are now required to declare the column names they return via the `column_names` property
- FlowAPI now reports whether a query is queued or running when polling
- FlowDB test data and synthetic data images are now available from their own Docker repos (Flowminder/flowdb-testdata, Flowminder/flowdb-synthetic-data)
- Changed query class name from `NocturnalCalls` to `NocturnalEvents`.

### Fixed
- FlowAPI is now an installable python module

### Removed
- Query objects can no longer be recalculated to cache and must be explicitly removed first
- Arbitrary `Flow` maths
- `EdgeList` query type
- Removes query class `ProportionOutgoing` as it becomes redundant with the the introduction of `ProportionEventType`.


## [0.3.0]
### Added
- API route for retrieving geography data from FlowDB
- Aggregated meaningful locations are now available via FlowAPI
- Origin-destination matrices between meaningful locations are now available via FlowAPI
- Added new `MeaningfulLocations`, `MeaningfulLocationsAggregate` and `MeaningfulLocationsOD` query classes to FlowMachine

### Changed

- Constructors for `HartiganCluster`, `LabelEventScore`, `EventScore` and `CallDays` now have different signatures 
- Restructured and extended documentation; added high-level overview and more targeted information for different types of users

## [0.2.2]
### Added
- Support for running FlowDB as an arbitrary user via docker's `--user` flag

### Removed
- Support for setting the uid and gid of the postgres user when building FlowDB

## [0.2.1]
### Fixed
- Fixed being unable to build if the port used by `git://` is not open

## [0.2.0]
### Added
- Added utilities for managing and inspecting the query cache

## [0.1.2]
### Changed
- FlowDB now requires a password to be set for the flowdb superuser

## [0.1.1]
### Added
- Support for password protected redis

### Changed
- Changed the default redis image to bitnami's redis (to enable password protection)

## [0.1.0]
### Added
- Added structured logging of access attempts, query running, and data access
- Added CHANGELOG.md
- Added support for Postgres JIT in FlowDB
- Added total location events metric to FlowAPI and FlowClient
- Added ETL bookkeeping schema to FlowDB

### Changed
- Added changelog update to PR template
- Increased default shared memory size for FlowDB containers

### Fixed
- Fixed being unable to delete groups in FlowAuth
- Fixed `make up` not working with defaults

## [0.0.5]
### Added
- Added Python 3.6 support for FlowClient


[Unreleased]: https://github.com/Flowminder/FlowKit/compare/0.6.1...master
[0.6.1]: https://github.com/Flowminder/FlowKit/compare/0.6.0...0.6.1
[0.6.0]: https://github.com/Flowminder/FlowKit/compare/0.5.3...0.6.0
[0.5.3]: https://github.com/Flowminder/FlowKit/compare/0.5.2...0.5.3
[0.5.2]: https://github.com/Flowminder/FlowKit/compare/0.5.1...0.5.2
[0.5.1]: https://github.com/Flowminder/FlowKit/compare/0.5.0...0.5.1
[0.5.0]: https://github.com/Flowminder/FlowKit/compare/0.4.3...0.5.0
[0.4.3]: https://github.com/Flowminder/FlowKit/compare/0.4.2...0.4.3
[0.4.2]: https://github.com/Flowminder/FlowKit/compare/0.4.1...0.4.2
[0.4.1]: https://github.com/Flowminder/FlowKit/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/Flowminder/FlowKit/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/Flowminder/FlowKit/compare/0.2.2...0.3.0
[0.2.2]: https://github.com/Flowminder/FlowKit/compare/0.2.1...0.2.2
[0.2.1]: https://github.com/Flowminder/FlowKit/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/Flowminder/FlowKit/compare/0.1.2...0.2.0
[0.1.2]: https://github.com/Flowminder/FlowKit/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/Flowminder/FlowKit/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/Flowminder/FlowKit/compare/0.0.5...0.1.0
[0.0.5]: https://github.com/Flowminder/FlowKit/compare/0.0.4...0.0.5
