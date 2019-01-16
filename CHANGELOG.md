# Changelog
All notable changes to FlowKit will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Added

### Changed
Use `https://` protocol instead of `git://` to clone postgres pldebugger repository in flowdb Dockerfile

### Fixed

### Removed

## [0.1.2]
### Added

### Changed
- FlowDB now requires a password to be set for the flowdb superuser

### Fixed

### Removed

## [0.1.1]
### Added
- Support for password protected redis

### Changed
- Changed the default redis image to bitnami's redis (to enable password protection)

### Fixed

### Removed

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

### Removed

## [0.0.5]
### Added
- Added Python 3.6 support for FlowClient

### Changed

### Fixed

### Removed


[Unreleased]: https://github.com/Flowminder/FlowKit/compare/0.1.2...master
[0.1.2]: https://github.com/Flowminder/FlowKit/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/Flowminder/FlowKit/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/Flowminder/FlowKit/compare/0.0.5...0.1.0
[0.0.5]: https://github.com/Flowminder/FlowKit/compare/0.0.4...0.0.5
