# Changelog
All notable changes to FlowKit will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Added
- Added new `ContactReferenceLocationStats` query class to FlowMachine.
- When running integration tests locally, normally pytest will automatically spin up servers for flowmachine and flowapi as part of the test setup.
  This can now be disabled by setting the environment variable `FLOWKIT_INTEGRATION_TESTS_DISABLE_AUTOSTART_SERVERS=TRUE`.
- The integration tests now use the environment variables `FLOWAPI_HOST`, `FLOWAPI_PORT` to determine how to connect to the flowapi server.

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

### Fixed
- Server no longer loses track of queries under heavy load

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


[Unreleased]: https://github.com/Flowminder/FlowKit/compare/0.4.2...master
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
