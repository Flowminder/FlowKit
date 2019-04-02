# Changelog
All notable changes to FlowKit will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Added
- A new zmq message `get_available_dates` was added to the flowmachine server, along with the `/available_dates`
  endpoint in flowapi and the function `get_available_dates()` in flowclient. These allow to determine the dates
  that are available in the database for the supported event types.

### Changed
- FlowMachine's debugging logs are now from a single logger (`flowmachine.debug`) and include the submodule in the submodule field instead of using it as the logger name
- FlowMachine's query run logger now uses the logger name `flowmachine.query_run_log`
- FlowAPI's access, run and debug loggers are now named `flowapi.access`, `flowapi.query` and `flowapi.debug`
- FlowAPI's access and run loggers, and FlowMachine's query run logger now log to stdout instead of stderr
- Passwords for Redis and FlowDB must now be explicitly provided to flowmachine via argument to `connect`, env var, or secret


### Fixed


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


[Unreleased]: https://github.com/Flowminder/FlowKit/compare/0.5.0...master
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
