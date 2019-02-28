# Changelog
All notable changes to FlowKit will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Added

- Dockerised development setup, with support for live reload of `flowmachine` and `flowapi` after source code changes.

### Changed
- `CustomQuery` now requires column names to be specified
- Query classes are now required to declare the column names they return via the `column_names` property
- FlowAPI now reports whether a query is queued or running when polling

### Fixed
- Server no longer loses track of queries under heavy load

### Removed
- Query objects can no longer be recalculated to cache and must be explicitly removed first


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


[Unreleased]: https://github.com/Flowminder/FlowKit/compare/0.3.0...master
[0.3.0]: https://github.com/Flowminder/FlowKit/compare/0.2.2...0.3.0
[0.2.2]: https://github.com/Flowminder/FlowKit/compare/0.2.1...0.2.2
[0.2.1]: https://github.com/Flowminder/FlowKit/compare/0.2.0...0.2.1
[0.2.0]: https://github.com/Flowminder/FlowKit/compare/0.1.2...0.2.0
[0.1.2]: https://github.com/Flowminder/FlowKit/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/Flowminder/FlowKit/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/Flowminder/FlowKit/compare/0.0.5...0.1.0
[0.0.5]: https://github.com/Flowminder/FlowKit/compare/0.0.4...0.0.5
