# Changelog

All notable changes to FlowKit will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

## [1.17.0]

### Changed
- __Action Needed__ Airflow updated to version 2.3.3; **backup flowetl_db before applying update** [#4940](https://github.com/Flowminder/FlowKit/pull/4940)
- Tables created under the cache schema in FlowDB will automatically be set to be owned by the `flowmachine` user. [#4714](https://github.com/Flowminder/FlowKit/issues/4714)
- `Query.explain` will now explain the query even where it is already stored. [#1285](https://github.com/Flowminder/FlowKit/issues/1285)
- `unstored_dependencies_graph` no longer blocks until dependencies are in a determinate state. [#4949](https://github.com/Flowminder/FlowKit/issues/4949)

### Removed
- `use_file_flux_sensor` removed entirely. [#2812](https://github.com/Flowminder/FlowKit/issues/2812)

## [1.16.0]

### Added
- Most frequent locations is now available via FlowAPI. [#3165](https://github.com/Flowminder/FlowKit/issues/3165)
- Total active periods is now available via FlowAPI.
- Made hour of day slicing available via FlowAPI. [#3165](https://github.com/Flowminder/FlowKit/issues/3165)
- Added visited on most days reference location query. [#4267](https://github.com/Flowminder/FlowKit/issues/4267)
- Added unique value from query list query. [#4486](https://github.com/Flowminder/FlowKit/pull/4486)
- Added mixin for exposing start_date and end_date internally as datetime objects [#4497](https://github.com/Flowminder/FlowKit/issues/4497)
- Added `CombineFirst` and `CoalescedLocation` queries. [#4524](https://github.com/Flowminder/FlowKit/issues/4524)
- Added `MajorityLocation` query. [#4522](https://github.com/Flowminder/FlowKit/issues/4522)
- Added `join_type` param to `Flows` class. [#4539](https://github.com/Flowminder/FlowKit/issues/4539)
- Added `PerSubscriberAggregate` query. [#4559](https://github.com/Flowminder/FlowKit/issues/4559)
- Added FlowETL QA checks 'count_imeis', 'count_imsis', 'count_locatable_location_ids', 'count_null_imeis', 'count_null_imsis', 'count_null_location_ids', 'max_msisdns_per_imei', 'max_msisdns_per_imsi', 'count_added_rows_outgoing', 'count_null_counterparts', 'count_null_durations', 'count_onnet_msisdns_incoming', 'count_onnet_msisdns_outgoing', 'count_onnet_msisdns', 'max_duration' and 'median_duration'. [#4552](https://github.com/Flowminder/FlowKit/issues/4552)
- Added `FilteredReferenceLocation` query, which returns only rows where a subscriber visited a reference location the required number of times. [#4584](https://github.com/Flowminder/FlowKit/issues/4584) 
- Added `LabelledSpatialAggregate` query and redaction, which sub-aggregates by subscriber labels. [#4668](https://github.com/Flowminder/FlowKit/issues/4668)
- Added `MobilityClassification` query, to classify subscribers by mobility type based on a sequence of locations. [#4666](https://github.com/Flowminder/FlowKit/issues/4666)
- Exposed `CoalescedLocation` via FlowAPI, in the specific case where the fallback location is a `FilteredReferenceLocation` query. [#4585](https://github.com/Flowminder/FlowKit/issues/4585)
- Added `LabelledFlows` query, which returns flows disaggregated by label [#4679](https://github.com/Flowminder/FlowKit/issues/4679)
- Exposed `LabelledSpatialAggregate` and `LabelledFlows` via FlowAPI, with a `MobilityClassification` query accepted as the 'labels' parameter. [#4669](https://github.com/Flowminder/FlowKit/issues/4669)
- Added `RedactedLabelledAggregate` and subclasses for redacting labelled data (see ADR 0011). [#4671](https://github.com/Flowminder/FlowKit/issues/4671)

### Changed
- Harmonised FlowAPI parameter names for start and end dates. They are now all `start_date` and `end_date`
- Further improvements to token display in FlowAuth. [#1124](https://github.com/Flowminder/FlowKit/issues/1124)
- Increased the FlowDB quickstart container's timeout to 15 minutes. [#782](https://github.com/Flowminder/FlowKit/issues/782)
- `Union` and `Query.union` now accept a variable number of queries to concatenate. [#4565](https://github.com/Flowminder/FlowKit/issues/4565)

### Fixed
- Autoflow's prefect version is now current. [#2544](https://github.com/Flowminder/FlowKit/issues/2544)
- FlowMachine server will now successfully remove cache for queries defined in an interactive flowmachine session during cleanup. [#4008](https://github.com/Flowminder/FlowKit/issues/4008)   

## [1.15.0]

### Added
- FlowETL flux check can be turned off by setting `use_flux_sensor=False` in `create_dag`. [#3603](https://github.com/Flowminder/FlowKit/issues/3603)

### Changed
- The `use_file_flux_sensor` argument to `create_dag` is deprecated. To use the table-based flux check in a file-based DAG, set `use_flux_sensor='table'`.
- Improvements to token display in FlowAuth. [#2812](https://github.com/Flowminder/FlowKit/issues/2812)

## [1.14.6]

### Added
- A list of additional paths to FlowETL QA checks can now be supplied to `create_dag` and `get_qa_checks`. [#3484](https://github.com/Flowminder/FlowKit/issues/3484)
- FlowETL docker container now includes the upgrade check script for Airflow 2.0.0.

### Fixed
- Additional FlowETL QA checks in the dags folder are now picked up. [#3484](https://github.com/Flowminder/FlowKit/issues/3484)
- Quickstart will no longer raise a warning about unset Autoflow related environment variables. [#2118](https://github.com/Flowminder/FlowKit/issues/2118)


## [1.14.5]
### Fixed
- FlowETL QA checks with template sections conditional on the `cdr_type` argument now render correctly. [#3479](https://github.com/Flowminder/FlowKit/issues/3479)

## [1.14.4]
### Fixed
- Fixed FlowClient ignoring custom SSL certificates [#3344](https://github.com/Flowminder/FlowKit/issues/3344)

## [1.14.3]

### Fixed
- Fixed FlowETL not using the randomly generated secret key to secure sessions with the web interface if one is not explicitly provided using `AIRFLOW__WEBSERVER__SECRET_KEY`. [#3244](https://github.com/Flowminder/FlowKit/issues/3244) 

## [1.14.2]

### Fixed
- Reinstated tabs navigation in the docs [#3238](https://github.com/Flowminder/FlowKit/issues/3238)
- Removed `$` from code snippets in developer docs [#3224](https://github.com/Flowminder/FlowKit/issues/3224)
- FlowETL now randomly generates a secret key to secure sessions with the web interface if one is not explicitly provided using `AIRFLOW__WEBSERVER__SECRET_KEY`. [#3244](https://github.com/Flowminder/FlowKit/issues/3244)

## [1.14.1]

### Fixed
- Docs displaying None where they shouldn't

## [1.14.0]

### Added
- Previously run, or currently running queries can now be referenced as a subscriber subset via FlowAPI. [#1009](https://github.com/Flowminder/FlowKit/issues/1009)
- total_network_objects, location_introversion, and unique_subscriber_counts now also accept subscriber subsets.
- The validity window for FlowAuth 2factor codes can now be configured using the `TWO_FACTOR_VALID_WINDOW` env variable. [#3203](https://github.com/Flowminder/FlowKit/issues/3203)

### Changed
- `get_cached_query_objects_ordered_by_score` is now a generator. [#3116](https://github.com/Flowminder/FlowKit/issues/3116)
- Flowclient now uses [httpx](https://www.python-httpx.org) instead of requests, for improved async performance and http2 support. [#1789](https://github.com/Flowminder/FlowKit/issues/1789)

### Fixed
- FlowAPI now correctly logs all query run, poll, and retrieval requests for matching with FlowMachine. [#3071](https://github.com/Flowminder/FlowKit/issues/3071)
- Links in the installation docs are now generated correctly. [#3152](https://github.com/Flowminder/FlowKit/issues/3152)

## [1.13.0]

### Changed
- When creating a file-based DAG using `create_dag`, you can now use the slower, table based method of checking whether the file is being written. [#2857](https://github.com/Flowminder/FlowKit/issues/2857)

## [1.12.0]

### Added

-   The issuer name can now be set for FlowAuth's 2factor authentication using the `FLOWAUTH_TWO_FACTOR_ISSUER` environment variable.
-   FlowAPI's internal port can now be set using the `FLOWAPI_PORT` environment variable, but continues to default to `9090`. [#2723](https://github.com/Flowminder/FlowKit/issues/2723)

    With thanks to [JIPS](https://www.jips.org) for supporting this work.
-   FlowETL's default port can now be set using the `FLOWETL_PORT` environment variable, but continues to default to `8080`. [#2724](https://github.com/Flowminder/FlowKit/issues/2724)

    With thanks to [JIPS](https://www.jips.org) for supporting this work.

### Changed
- Test and synthetic DFS data now uses the same pool of subscribers as CDR data. [#2713](https://github.com/Flowminder/FlowKit/issues/2713)
  
  With thanks to [JIPS](https://www.jips.org) for supporting this work.

## [1.11.1]

### Added

-   FlowDB's SQL synthetic data generator now uses the [WorldPop project](https://www.worldpop.org)'s [2016 population raster](https://www.worldpop.org/doi/10.5258/SOTON/WP00647) for the country chosen as the basis for generating data.

## [1.11.0]

### Added

-   Queries run through FlowAPI can now be run on only a subset of the available CDR types, by supplying an `event_types` parameter. [#2631](https://github.com/Flowminder/FlowKit/issues/2631)
-   FlowETL now includes QA checks for the earliest and latest timestamps in the ingested data. [#2627](https://github.com/Flowminder/FlowKit/issues/2627)

### Fixed

-   The FlowETL 'count_duplicates' QA check now correctly counts the number of duplicate rows. [#2651](https://github.com/Flowminder/FlowKit/issues/2651)

## [1.10.0]

### Added

-   FlowDB's SQL synthetic data generator can now generate events for any country, not just Nepal.

    To generate synthetic data for a different country, supply the `COUNTRY` environment variable when starting the container, and a valid GADM GID code for the region to simulate a disaster.

### Changed

-   FlowMachine's docker container now uses Python 3.8
-   FlowAPI's docker container now uses Python 3.8
-   FlowAuth's docker container now uses Python 3.8
-   AutoFlow's docker container now uses Python 3.8
-   FlowDB's SQL synthetic data generator now uses [GADM 3.6](https://gadm.org) boundaries.
-   FlowAuth and FlowAPI now exchange tokens with compressed claims. [#2625](https://github.com/Flowminder/FlowKit/issues/2625)

### Fixed

-   FlowAuth will no longer fail to start if there are directories with names the same as the SSL certificate secrets.

## [1.9.4]

## Changed

-   `JoinToLocation` is cacheable only if the joined query is also cacheable.

## [1.9.3]

### Changed

-   `SubscriberLocations` are no longer cacheable using FlowMachine.

### Fixed

-   Fixed cache shrinking failing when large numbers of tables have been written. [#2462](https://github.com/Flowminder/FlowKit/issues/2462)
-   Fixed FlowAuth's MySQL support.

## [1.9.2]

### Fixed

-   Added missing bridge table arguments to Several FlowClient methods.

## [1.9.1]

### Added

-   FlowAuth now supports MySQL as a database backend.
-   FlowKit now allows the use of bridge tables to manually specify linkages between cells and geometries.

### Fixed

-   FlowAuth no longer errors after a period of inactivity due to timed out database connections. [#2382](https://github.com/Flowminder/FlowKit/issues/2382)

## [1.9.0]

### Added

-   Added new FlowAPI aggregates; `unique_visitor_counts`, `active_at_reference_location_counts`, `unmoving_counts`, `unmoving_at_reference_location_counts`, `trips_od_matrix`, and `consecutive_trips_od_matrix`
-   Added new Flows type query to FlowAPI `unique_locations`, which produces the paired regional connectivity [COVID-19 indicator](https://github.com/Flowminder/COVID-19/blob/master/od_matrix_undirected_all_pairs.md)
-   Added FlowClient function `unique_locations_spec`, which can be used on either side of a `flows` query
-   Added FlowClient functions: `unique_visitor_counts`, `active_at_reference_location_counts`, `unmoving_counts`, `unmoving_at_reference_location_counts`, `trips_od_matrix`, and `consecutive_trips_od_matrix`. [#2333](https://github.com/Flowminder/FlowKit/issues/2333)
-   FlowClient now has an asyncio API. Use `connect_async` instead of `connect` to create an `ASyncConnection`, and `await` methods on `APIQuery` objects. [#2199](https://github.com/Flowminder/FlowKit/issues/2199)

### Fixed

-   Fixed FlowMachine server becoming deadlocked under load. [#2390](https://github.com/Flowminder/FlowKit/issues/2390)

## [1.8.0]

### Added

-   Added subscriber metrics: `ActiveAtReferenceLocation`, `Unmoving`, `UnmovingAtReferenceLocation` and `UniqueLocations`
-   Added location metrics and their `Redacted*` equivalents:
    -   `UniqueVisitorCounts`
    -   `UnmovingAtReferenceLocationCounts` ([COVID-19 equivalent](https://github.com/Flowminder/COVID-19/issues/8))
    -   `ActiveAtReferenceLocationCounts`
    -   `UnmovingCount` ([COVID-19 equivalent](https://github.com/Flowminder/COVID-19/issues/10))
    -   `TripsODMatrix` ([COVID-19 equivalent](https://github.com/Flowminder/COVID-19/blob/c1a4d2009cb0dd2aac1c3cc4b1527fa99f474fca/od_matrix_directed_all_pairs.md))
    -   `ConsecutiveTripsODMatrix` ([COVID-19 equivalent](https://github.com/Flowminder/COVID-19/issues/9))
        See https://covid19.flowminder.org for more detail on how [Flowminder](https://flowminder.org) is supporting the global COVID-19 response.

## [1.7.0]

### Changed

-   FlowETL is now based on the official apache-airflow docker image. As a result, you should now bind mount your host dags directory to `/opt/airflow/dags`, and your logs directory to `/opt/airflow/logs`.

## [1.6.1]

### Fixed

-   FlowMachine server will now ignore values for the `FLOWMACHINE_SERVER_THREADPOOL_SIZE` environment variable which can't be cast to `int`. [#2304](https://github.com/Flowminder/FlowKit/issues/2304)

## [1.6.0]

### Added

-   `histogram_aggregate` added to FlowAPI and FlowClient. Allows the user to obtain a histogram over a per-subscriber metric. [#1076](https://github.com/Flowminder/FlowKit/issues/1076)

## [1.5.1]

### Added

-   FlowClient now displays a progress bar when waiting for a query to ready, indicating how many parts of that query still need to be run.

## [1.5.0]

### Added

-   Added a flowclient `Query` class to represent a FlowKit query [#1980](https://github.com/Flowminder/FlowKit/issues/1980).
-   Added method `flowclient.Connection.update_token`, to replace the API token for an existing connection.

### Changed

-   The names of flowclient functions for generating query specifications have been renamed to `<previous_name>_spec` (e.g. `flowclient.modal_location` is now `flowclient.modal_location_spec`).
-   `flowclient.get_status` now returns `"not_running"` (instead of raising `FileNotFoundError`) if a query is not running or completed.
-   Flowclient functions `location_event_counts_spec`, `meaningful_locations_aggregate_spec`, `meaningful_locations_between_label_od_matrix_spec`, `meaningful_locations_between_dates_od_matrix_spec`, `flows_spec`, `unique_subscriber_counts_spec`, `location_introversion_spec`, `total_network_objects_spec`, `aggregate_network_objects_spec`, `spatial_aggregate_spec` and `joined_spatial_aggregate_spec` have moved to the `flowclient.aggregates` submodule.

## [1.4.0]

### Added

-   FlowAPI can now return results in CSV and GeoJSON format, FlowClient now supports getting GeoJSON formatted results. [#2003](https://github.com/Flowminder/FlowKit/issues/2003)

## [1.3.3]

### Added

-   FlowAPI now reports the proportion of subqueries cached for a query when polling. [#1202](https://github.com/Flowminder/FlowKit/issues/1202)
-   FlowClient now logs info messages with the proportion of subqueries cached for a query when polling. [#1202](https://github.com/Flowminder/FlowKit/issues/1202)

### Fixed

-   Fixed the display of deeply nested permissions for flows in FlowAuth. [#2110](https://github.com/Flowminder/FlowKit/issues/2110)

## [1.3.2]

### Fixed

-   Fixed tokens which used the FlowAuth demo data not being accepted by FlowAPI. [#2108](https://github.com/Flowminder/FlowKit/issues/2108)

## [1.3.1]

### Changed

-   Flowmachine now uses an enum for interaction direction parameters (but will still accept them as strings). [#357](https://github.com/Flowminder/FlowKit/issues/357)

### Removed

-   Removed unused aggregates, results and features schemas from FlowDB. [#587](https://github.com/Flowminder/FlowKit/issues/587)

## [1.3.0]

### Added

-   Improved UI for API permissions in FlowAuth.

### Changed

-   The format of user claims expected has changed from a dictionary, to string based format. FlowAPI now expects the claims key of any token to contain a list of scope strings.
-   Permissions for joined spatial aggregates can now be set at a finer level in FlowAuth, to allow administrators to grant access only to specific combinations of query types at different aggregation units.
-   FlowAuth no longer requires administrators to manually configure API routes, and will extract them from a FlowAPI server's open api specification.
-   FlowAuth now uses structlog for log messages.
-   FlowAPI no longer mandates a top level `aggregation_unit` field in query specifications.
-   FlowClient's `flows` and `modal_location` functions no longer require an aggregation unit.

### Removed

-   The poll type permission has been removed, and is implicitly granted by both read and get_result rights.
-   FlowAuth no longer allows administrators to specify the name of a FlowAPI server, and will instead use the name specified in the server's open api specification.

## [1.2.1]

## Fixed

-   Queries which have been removed Flowmachine's cache, or cancelled can now be rerun. [#1898](https://github.com/Flowminder/FlowKit/issues/1898)

## [1.2.0]

### Added

-   FlowMachine can now use multiple FlowDB backends, redis instances or execution pools via the `flowmachine.connections` or `flowmachine.core.context.context` context managers. [#391](https://github.com/Flowminder/FlowKit/issues/391)
-   `flowmachine.core.connection.Connection` now has a `conn_id` attribute, which is unique per database host. [#391](https://github.com/Flowminder/FlowKit/issues/391)

### Changed

-   `flowmachine.connect` no longer returns a `Connection` object. The connection should be accessed via `flowmachine.core.context.get_db()`. [#391](https://github.com/Flowminder/FlowKit/issues/391)
-   `connection`, `redis`, and `threadpool` are no longer available as attributes of `Query`, and should be accessed via `flowmachine.core.context.get_db()`, `flowmachine.core.context.get_redis()` and `flowmachine.core.context.get_executor()`. [#391](https://github.com/Flowminder/FlowKit/issues/391)

### Removed

-   Removed `Query.connection`, `Query.redis`, and `Query.threadpool`. [#391](https://github.com/Flowminder/FlowKit/issues/391)

## [1.1.1]

### Added

-   Added a worked example to demonstrate using joined spatial aggregate queries. [#1938](https://github.com/Flowminder/FlowKit/issues/1938)

## [1.1.0]

### Changed

-   `Connection.available_dates` is now a property and returns results based on the `etl.etl_records` table. [#1873](https://github.com/Flowminder/FlowKit/issues/1873)

### Fixed

-   Fixed the run action blocking the FlowMachine server in some scenarios. [#1256](https://github.com/Flowminder/FlowKit/issues/1256)

### Removed

-   Removed `tables` and `columns` methods from the `Connection` class in FlowMachine
-   Removed the `inspector` attribute from the `Connection` class in FlowMachine

## [1.0.0]

### Added

-   FlowMachine now periodically prunes the cache to below the permitted cache size. [#1307](https://github.com/Flowminder/FlowKit/issues/1307)
    The frequency of this pruning is configurable using the `FLOWMACHINE_CACHE_PRUNING_FREQUENCY` environment variable to Flowmachine, and queries are excluded from being removed by the automatic shrinker based on the `cache_protected_period` config key within FlowDB.
-   FlowDB now includes Paul Ramsey's [OGR foreign data wrapper](https://github.com/pramsey/pgsql-ogr-fdw), for easy loading of GIS data. [#1512](https://github.com/Flowminder/FlowKit/issues/1512)
-   FlowETL now allows all configuration options to be set using docker secrets. [#1515](https://github.com/Flowminder/FlowKit/issues/1515)
-   Added a new component, AutoFlow, to automate running Jupyter notebooks when new data is added to FlowDB. [#1570](https://github.com/Flowminder/FlowKit/issues/1570)
-   `FLOWETL_INTEGRATION_TESTS_SAVE_AIRFLOW_LOGS` environment variable added to allow copying the Airflow logs in FlowETL integration tests into the /mounts/logs directory for debugging. [#1019](https://github.com/Flowminder/FlowKit/issues/1019)
-   Added new `IterativeMedianFilter` query to Flowmachine, which applies an iterative median filter to the output of another query. [#1339](https://github.com/Flowminder/FlowKit/issues/1339)
-   FlowDB now includes the [TDS foreign data wrapper](https://github.com/tds-fdw). [#1729](https://github.com/Flowminder/FlowKit/issues/1729)
-   Added contributing and support instructions. [#1791](https://github.com/Flowminder/FlowKit/issues/1791)
-   New FlowETL module installable via pip to aid in ETL dag creation.

### Changed

-   FlowDB is now built on PostgreSQL 12 [#1396](https://github.com/Flowminder/FlowKit/issues/1313) and PostGIS 3.
-   FlowETL is now built on Airflow [10.1.6](https://airflow.apache.org/changelog.html#airflow-1-10-6-2019-10-28).
-   FlowETL now defaults to disabling Airflow's REST API, and enables RBAC for the webui. [#1516](https://github.com/Flowminder/FlowKit/issues/1516)
-   FlowETL now requires that the `FLOWETL_AIRFLOW_ADMIN_USERNAME` and `FLOWETL_AIRFLOW_ADMIN_PASSWORD` environment variables be set, which specify the default web ui account. [#1516](https://github.com/Flowminder/FlowKit/issues/1516)
-   FlowAPI will no longer return a result for rows in spatial aggregate, joined spatial aggregate, flows, total events, meaningful locations aggregate, meaningful locations od, or unique subscriber count where the aggregate would contain less than 16 sims. [#1026](https://github.com/Flowminder/FlowKit/issues/1026)
-   FlowETL now requires that `AIRFLOW__CORE__SQL_ALCHEMY_CONN` be provided as an environment variable or secret. [#1702](https://github.com/Flowminder/FlowKit/issues/1702), [#1703](https://github.com/Flowminder/FlowKit/issues/1703)
-   FlowAuth now records last used two-factor authentication codes in an expiring cache, which supports either a file-based, or redis backend. [#1173](https://github.com/Flowminder/FlowKit/issues/1173)
-   AutoFlow now uses [Bundler](https://bundler.io/) to manage Ruby dependencies.
-   The `end_date` parameter of `flowclient.modal_location_from_dates` now refers to the day _after_ the final date included in the range, so is now consistent with other queries that have start/end date parameters. [#819](https://github.com/Flowminder/FlowKit/issues/819)
-   Date intervals in AutoFlow date stencils are now interpreted as half-open intervals (i.e. including start date, excluding end date), for consistency with date ranges elsewhere in FlowKit.
-   `flowmachine` user now has read access to ETL metadata tables in FlowDB

### Fixed

-   Quickstart should no longer fail on systems which do not include the `netstat` tool. [#1472](https://github.com/Flowminder/FlowKit/issues/1472)
-   Fixed an error that prevented FlowAuth admin users from resetting users' passwords using the FlowAuth UI. [#1635](https://github.com/Flowminder/FlowKit/issues/1635)
-   The 'Cancel' button on the FlowAuth 'New User' form no longer submits the form. [#1636](https://github.com/Flowminder/FlowKit/issues/1636)
-   FlowAuth backend now sends a meaningful 400 response when trying to create a user with an empty password. [#1637](https://github.com/Flowminder/FlowKit/issues/1637)
-   Usernames of deleted users can now be re-used as usernames for new users. [#1638](https://github.com/Flowminder/FlowKit/issues/1638)
-   RedactedJoinedSpatialAggregate now only redacts rows with too few subscribers. [#1747](https://github.com/Flowminder/FlowKit/issues/1747)
-   FlowDB now uses a more conservative default setting for `tcp_keepalives_idle` of 10 minutes, to avoid connections being killed after 15 minutes when running in a docker swarm. [#1771](https://github.com/Flowminder/FlowKit/issues/1771)
-   Aggregation units and api routes can now be added to servers. [#1815](https://github.com/Flowminder/FlowKit/issues/1815)
-   Fixed several issues with FlowETL. [#1529](https://github.com/Flowminder/FlowKit/issues/1529) [#1499](https://github.com/Flowminder/FlowKit/issues/1499) [#1498](https://github.com/Flowminder/FlowKit/issues/1498) [#1497](https://github.com/Flowminder/FlowKit/issues/1497)

### Removed

-   Removed pg_cron.

## [0.9.1]

### Added

-   Added new `DistanceSeries` query to Flowmachine, which produces per-subscriber time series of distance from a reference point. [#1313](https://github.com/Flowminder/FlowKit/issues/1313)
-   Added new `ImputedDistanceSeries` query to Flowmachine, which produces contiguous per-subscriber time series of distance from a reference point by filling in gaps using the rolling median. [#1337](https://github.com/Flowminder/FlowKit/issues/1337)

### Changed

### Fixed

-   The FlowETL config file is now always validated, avoiding runtime errors if a config setting is wrong or missing. [#1375](https://github.com/Flowminder/FlowKit/issues/1375)
-   FlowETL now only creates DAGs for CDR types which are present in the config, leading to a better user experience in the Airflow UI. [#1376](https://github.com/Flowminder/FlowKit/issues/1376)
-   The `concurrency` settings in the FlowETL config are no longer ignored. [#1378](https://github.com/Flowminder/FlowKit/issues/1378)
-   The FlowETL deployment example has been updated so that it no longer fails due to a missing foreign data wrapper for the available CDR dates. [#1379](https://github.com/Flowminder/FlowKit/issues/1379)
-   Fixed error when editing a user in FlowAuth who did not have two factor enabled. [#1374](https://github.com/Flowminder/FlowKit/issues/1374)
-   Fixed not being able to enable a newly added api route on existing servers in FlowAuth. [#1373](https://github.com/Flowminder/FlowKit/issues/1373)

### Removed

-   The `default_args` section in the FlowETL config file has been removed. [#1377](https://github.com/Flowminder/FlowKit/issues/1377)

## [0.9.0]

### Added

-   FlowAuth now makes version information available at `/version` and displays it in the web ui. [#835](https://github.com/Flowminder/FlowKit/issues/835)
-   FlowETL now comes with a deployment example (in `flowetl/deployment_example/`). [#1126](https://github.com/Flowminder/FlowKit/issues/1126)
-   FlowETL now allows to run supplementary post-ETL queries. [#989](https://github.com/Flowminder/FlowKit/issues/989)
-   Random sampling is now exposed via the API, for all non-aggregated query kinds. [#1007](https://github.com/Flowminder/FlowKit/issues/1007)
-   New aggregate added to FlowMachine - `HistogramAggregation`, which constructs histograms over the results of other queries. [#1075](https://github.com/Flowminder/FlowKit/issues/1075)
-   New `IntereventInterval` query class - returns stats over the gap between events as a time interval.
-   Added submodule `flowmachine.core.dependency_graph`, which contains functions related to creating or using query dependency graphs (previously these were in `utils.py`).
-   New config option `sql_find_available_dates` in FlowETL to provide SQL code to determine the available dates. [#1295](https://github.com/Flowminder/FlowKit/issues/1295)

### Changed

-   FlowDB is now based on PostgreSQL 11.5 and PostGIS 2.5.3
-   When running queries through FlowAPI, the query's dependencies will also be cached by default. This behaviour can be switched off by setting `FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING=true`. [#1152](https://github.com/Flowminder/FlowKit/issues/1152)
-   `NewSubscribers` now takes a pair of `UniqueSubscribers` queries instead of the arguments to them
-   Flowmachine's default random sampling method is now `random_ids` rather than the non-reproducible `system_rows`. [#1263](https://github.com/Flowminder/FlowKit/issues/1263)
-   `IntereventPeriod` now returns stats over the gap between events in fractional time units, instead of time intervals. [#1265](https://github.com/Flowminder/FlowKit/issues/1265)
-   Attempting to store a query that does not have a standard table name (e.g. `EventTableSubset` or unseeded random sample) will now raise an `UnstorableQueryError` instead of `ValueError`.
-   In the FlowETL deployment example, the external ingestion database is now set up separately from the FlowKit components and connected to FlowDB via a docker overlay network. [#1276](https://github.com/Flowminder/FlowKit/issues/1276)
-   The `md5` attribute of the `Query` class has been renamed to `query_id` [#1288](https://github.com/Flowminder/FlowKit/issues/1288).
-   `DistanceMatrix` no longer returns duplicate rows for the lon-lat spatial unit.
-   Previously, `Displacement` defaulted to returning `NaN` for subscribers who have a location in the reference location but were not seen in the time period for the displacement query. These subscribers are no longer returned unless the `return_subscribers_not_seen` argument is set to `True`.
-   `PopulationWeightedOpportunities` is now available under `flowmachine.features.location`, instead of `flowmachine.models`
-   `PopulationWeightedOpportunities` no longer supports erroring with incomplete per-location departure rate vectors and will instead omit any locations not included from the results
-   `PopulationWeightedOpportunities` no longer requires use of the `run()` method

### Fixed

-   Quickstart will no longer fail if it has been run previously with a different FlowDB data size and not explicitly shut down. [#900](https://github.com/Flowminder/FlowKit/issues/900)

### Removed

-   Flowmachine's `subscriber_locations_cluster` function has been removed - use `HartiganCluster` or `MeaningfulLocations` directly.
-   FlowAPI no longer supports the non-reproducible random sampling method `system_rows`. [#1263](https://github.com/Flowminder/FlowKit/issues/1263)

## [0.8.0]

### Added

-   FlowAPI's 'joined_spatial_aggregate' endpoint now exposes event counts. [#992](https://github.com/Flowminder/FlowKit/issues/992)
-   FlowAPI's 'joined_spatial_aggregate' endpoint now exposes top-up amount. [#967](https://github.com/Flowminder/FlowKit/issues/967)
-   FlowAPI's 'joined_spatial_aggregate' endpoint now exposes nocturnal events. [#1025](https://github.com/Flowminder/FlowKit/issues/1025)
-   FlowAPI's 'joined_spatial_aggregate' endpoint now exposes top-up balance. [#968](https://github.com/Flowminder/FlowKit/issues/968)
-   FlowAPI's 'joined_spatial_aggregate' endpoint now exposes displacement. [#1010](https://github.com/Flowminder/FlowKit/issues/1010)
-   FlowAPI's 'joined_spatial_aggregate' endpoint now exposes pareto interactions. [#1012](https://github.com/Flowminder/FlowKit/issues/1012)
-   FlowETL now supports ingesting from a postgres table in addition to CSV files. [#1027](https://github.com/Flowminder/FlowKit/issues/1027)
-   `FLOWETL_RUNTIME_CONFIG` environment variable added to control which DAG definitions the FlowETL integration tests should use (valid values: "testing", "production").
-   `FLOWETL_INTEGRATION_TESTS_DISABLE_PULLING_DOCKER_IMAGES` environment variable added to allow running the FlowETL integration tests against locally built docker images during development.
-   FlowAPI's 'joined_spatial_aggregate' endpoint now exposes handset. [#1011](https://github.com/Flowminder/FlowKit/issues/1011) and [#1029](https://github.com/Flowminder/FlowKit/issues/1029)
-   `JoinedSpatialAggregate` now supports "distr" stats which computes outputs the relative distribution of the passed metrics.
-   Added `SubscriberHandsetCharacteristic` to FlowMachine
-   FlowAuth now supports optional two-factor authentication [#121](https://github.com/Flowminder/FlowKit/issues/121)

### Changed

-   The flowdb containers for test_data and synthetic_data were split into two separate containers and quick_start.sh downloads the docker-compose files to a new temporary directory on each run. [#843](https://github.com/Flowminder/FlowKit/issues/843)
-   Flowmachine now returns more informative error messages when query parameter validation fails. [#1055](https://github.com/Flowminder/FlowKit/issues/1055)

### Removed

-   `TESTING` environment variable was removed (previously used by the FlowETL integration tests).
-   Removed `SubscriberPhoneType` from FlowMachine to avoid redundancy.

## [0.7.0]

### Added

-   `PRIVATE_JWT_SIGNING_KEY` environment variable/secret added to FlowAuth, which should be a PEM encoded RSA private key, optionally base64 encoded if supplied as an environment variable.
-   `PUBLIC_JWT_SIGNING_KEY` environment variable/secret added to FlowAPI, which should be a PEM encoded RSA public key, optionally base64 encoded if supplied as an environment variable.
-   The dev provisioning Ansible playbook now automatically generates an SSH key pair for the `flowkit` user. [#892](https://github.com/Flowminder/FlowKit/issues/892)
-   Added new classes to represent spatial units in FlowMachine.
-   Added a `Geography` query class, to get geography data for a spatial unit.
-   FlowAPI's 'joined_spatial_aggregate' endpoint now exposes unique location counts.[#949](https://github.com/Flowminder/FlowKit/issues/949)
-   FlowAPI's 'joined_spatial_aggregate' endpoint now exposes subscriber degree.[#969](https://github.com/Flowminder/FlowKit/issues/969)
-   Flowdb now contains an auxiliary table to record outcomes of queries that can be run as part of the regular ETL process [#988](https://github.com/Flowminder/FlowKit/issues/988)

### Changed

-   The quick-start script now only pulls the docker images for the services that are actually started up. [#898](https://github.com/Flowminder/FlowKit/issues/898)
-   FlowAuth and FlowAPI are now linked using an RSA keypair, instead of per-server shared secrets. [#89](https://github.com/Flowminder/FlowKit/issues/89)
-   Location-related FlowMachine queries now take a `spatial_unit` parameter instead of `level`.
-   The quick-start script now uses the environment variable `GIT_REVISION` to control the version to be deployed.
-   Create token page permission and spatial aggregation checkboxes are now hidden by default.[#834](https://github.com/Flowminder/FlowKit/issues/834)
-   The flowetl mounted directories `archive, dump, ingest, quarantine` were replaced with a single `files` directory and files are no longer moved. [#946](https://github.com/Flowminder/FlowKit/issues/946)
-   FlowDB's postgresql has been updated to [11.4](https://www.postgresql.org/about/news/1949/), which addresses several bugs and one major vulnerability.

### Fixed

-   When creating a new token in FlowAuth, the expiry now always shows the year, seconds till expiry, and timezone. [#260](https://github.com/Flowminder/FlowKit/issues/260)
-   Distances in `Displacement` are now calculated with longitude and latitude the corrcet way around. [#913](https://github.com/Flowminder/FlowKit/issues/913)
-   The quick-start script now works correctly with branches. [#902](https://github.com/Flowminder/FlowKit/issues/902)
-   Fixed `location_event_counts` failing to work when specifying a subset of event types [#1015](https://github.com/Flowminder/FlowKit/issues/1015)
-   FlowAPI will now show the correct version in the API spec, flowmachine and flowclient will show the correct versions in the worked examples. [#818](https://github.com/Flowminder/FlowKit/issues/818)

### Removed

-   Removed `cell_mappings.py`, `get_columns_for_level` and `BadLevelError`.

-   `JWT_SECRET_KEY` has been removed in favour of RSA keys.
-   The FlowDB tables `infrastructure.countries` and `infrastructure.operators` have been removed. [#958](https://github.com/Flowminder/FlowKit/issues/958)

## [0.6.4]

### Added

-   Buttons to copy token to clipboard and download token as file added to token list page. [#704](https://github.com/Flowminder/FlowKit/issues/704)
-   Two new worked examples: "Cell Towers Per Region" and "Unique Subscriber Counts". [#633](https://github.com/Flowminder/FlowKit/issues/633), [#634](https://github.com/Flowminder/FlowKit/issues/634)

### Changed

-   The `FLOWDB_DEBUG` environment variable has been renamed to `FLOWDB_ENABLE_POSTGRES_DEBUG_MODE`.
-   FlowAuth will now automatically set up the database when started without needing to trigger via the cli.
-   FlowAuth now requires that at least one administrator account is created by providing env vars or secrets for:
    -   `FLOWAUTH_ADMIN_PASSWORD`
    -   `FLOWAUTH_ADMIN_USERNAME`

### Fixed

-   The `FLOWDB_DEBUG` environment variable used to have no effect. This has been fixed. [#811](https://github.com/Flowminder/FlowKit/issues/811)
-   Previously, queries could be stuck in an executing state if writing their cache metadata failed, they will now correctly show as having errored. [#833](https://github.com/Flowminder/FlowKit/issues/833)
-   Fixed an issue where `Table` objects could be in an inconsistent cache state after resetting cache [#832](https://github.com/Flowminder/FlowKit/issues/832)
-   FlowAuth's docker container can now be used with a Postgres backing database. [#825](https://github.com/Flowminder/FlowKit/issues/825)
-   FlowAPI now starts up successfully when following the "Secrets Quickstart" instructions in the docs. [#836](https://github.com/Flowminder/FlowKit/issues/836)
-   The command to generate an SSL certificate in the "Secrets Quickstart" section in the docs has been fixed and made more robust [#837](https://github.com/Flowminder/FlowKit/issues/837)
-   FlowAuth will no longer try to initialise the database or create demo data multiple times when running under uwsgi with multiple workers [#844](https://github.com/Flowminder/FlowKit/issues/844)
-   Fixed issue of Multiple tokens don't line up on FlowAuth "Tokens" page [#849](https://github.com/Flowminder/FlowKit/issues/849)

### Removed

-   The `FLOWDB_SERVICES` environment variable has been removed from the toplevel Makefile, so that now `DOCKER_SERVICES` is the only environment variable that controls which services are spun up when running `make up`. [#827](https://github.com/Flowminder/FlowKit/issues/827)

## [0.6.3]

### Added

-   FlowKit's worked examples are now Dockerized, and available as part of the quick setup script [#614](https://github.com/Flowminder/FlowKit/issues/614)
-   Skeleton for Airflow based ETL system added with basic ETL DAG specification and tests.
-   The docs now contain information about required versions of installation prerequisites [#703](https://github.com/Flowminder/FlowKit/issues/703)
-   FlowAPI now requires the `FLOWAPI_IDENTIFIER` environment variable to be set, which contains the name used to identify this FlowAPI server when generating tokens in FlowAuth [#727](https://github.com/Flowminder/FlowKit/issues/727)
-   `flowmachine.utils.calculate_dependency_graph` now includes the `Query` objects in the `query_object` field of the graph's nodes dictionary [#767](https://github.com/Flowminder/FlowKit/issues/767)
-   Architectural Decision Records (ADR) have been added and are included in the auto-generated docs [#780](https://github.com/Flowminder/FlowKit/issues/780)
-   Added FlowDB environment variables `SHARED_BUFFERS_SIZE` and `EFFECTIVE_CACHE_SIZE`, to allow manually setting the Postgres configuration parameters `shared_buffers` and `effective_cache_size`.
-   The function `print_dependency_tree()` now takes an optional argument `show_stored` to display information whether dependent queries have been stored or not [#804](https://github.com/Flowminder/FlowKit/issues/804)
-   A new function `plot_dependency_graph()` has been added which allows to conveniently plot and visualise a dependency graph for use in Jupyter notebooks (this requires IPython and pygraphviz to be installed) [#786](https://github.com/Flowminder/FlowKit/issues/786)

### Changed

-   Parameter names in `flowmachine.connect()` have been renamed as follows to be consistent with the associated environment variables [#728](https://github.com/Flowminder/FlowKit/issues/728):
    -   `db_port -> flowdb_port`
    -   `db_user -> flowdb_user`
    -   `db_pass -> flowdb_password`
    -   `db_host -> flowdb_host`
    -   `db_connection_pool_size -> flowdb_connection_pool_size`
    -   `db_connection_pool_overflow -> flowdb_connection_pool_overflow`
-   FlowAPI and FlowAuth now expect an audience key to be present in tokens [#727](https://github.com/Flowminder/FlowKit/issues/727)
-   Dependent queries are now only included once in the md5 calculation of a given query (in particular, it changes the query ids compared to previous FlowKit versions).
-   Error is displayed in the add user form of Flowauth if username is alredy exists. [#690](https://github.com/Flowminder/FlowKit/issues/690)
-   Error is displayed in the add group form of Flowauth if group name already exists. [#709](https://github.com/Flowminder/FlowKit/issues/709)
-   FlowAuth's add new server page now shows helper text for bad inputs. [#749](https://github.com/Flowminder/FlowKit/pull/749)
-   The class `SubscriberSubsetterBase` in FlowMachine no longer inherits from `Query` [#740](https://github.com/Flowminder/FlowKit/issues/740) (this changes the query ids compared to previous FlowKit versions).

### Fixed

-   FlowClient docs rendered to website now show the options available for arguments that require a string from some set of possibilities [#695](https://github.com/Flowminder/FlowKit/issues/695).
-   The Flowmachine loggers are now initialised only once when flowmachine is imported, with a call to `connect()` only changing the log level [#691](https://github.com/Flowminder/FlowKit/issues/691)
-   The FERNET_KEY environment variable for FlowAuth is now named FLOWAUTH_FERNET_KEY
-   The quick-start script now correctly aborts if one of the FlowKit services doesn't fully start up [#745](https://github.com/Flowminder/flowkit/issues/745)
-   The maps in the worked examples docs pages now appear in any browser
-   Example invocations of `generate-jwt` are no longer uncopyable due to line wrapping [#778](https://github.com/Flowminder/flowkit/issues/745)
-   API parameter `interval` for `location_event_counts` queries is now correctly passed to the underlying FlowMachine query object [#807](https://github.com/Flowminder/FlowKit/issues/807).

## [0.6.2]

### Added

-   Added a new module, `flowkit-jwt-generator`, which generates test JWT tokens for use with FlowAPI [#564](https://github.com/Flowminder/FlowKit/issues/564)
-   A new Ansible playbook was added in `deployment/provision-dev.yml`. In addition to the standard provisioning
    this installs pyenv, Python 3.7, pipenv and clones the FlowKit repository, which is useful for development purposes.
-   Added a 'quick start' setup script for trying out a complete FlowKit system [#688](https://github.com/Flowminder/FlowKit/issues/688).

### Changed

-   FlowAPI's `available_dates` endpoint now always returns available dates for all event types and does not accept JSON
-   Hints are now displayed in the add user form of FlowAuth if the form is not completed [#679](https://github.com/Flowminder/FlowKit/issues/679)
-   Error messages are now displayed when generating a new token in FlowAuth if the token's name is invalid [#799](https://github.com/Flowminder/FlowKit/issues/799)
-   The Ansible playbooks in `deployment/` now allow configuring the username and password for the FlowKit user account.
-   Default compose file no longer includes build blocks, these have been moved to `docker-compose-build.yml`.

### Fixed

-   FlowDB synthetic data container no longer silently fails to generate data if data generator is not set [#654](https://github.com/Flowminder/FlowKit/issues/654)

## [0.6.1]

### Fixed

-   Fixed `TotalNetworkObjects` raising an error when run with a lat-long level [#108](https://github.com/Flowminder/FlowKit/issues/108)
-   Radius of gyration no longer incorrectly appears as a top level api query

## [0.6.0]

### Added

-   Added new flowclient API entrypoint, `aggregate_network_objects`, to access equivalent flowmachine query [#601](https://github.com/Flowminder/FlowKit/issues/601)
-   FlowAPI now exposes the API spec at the `spec/openapi.json` endpoint, and an interactive version of the spec at the `spec/redoc` endpoint
-   Added Makefile target `make up-no_build`, to spin up all containers without building the images
-   Added `resync_redis_with_cache` function to cache utils, to allow administrators to align redis with FlowDB [#636](https://github.com/Flowminder/FlowKit/issues/636)
-   Added new flowclient API entrypoint, `radius_of_gyration`, to access (with simplified parameters) equivalent flowmachine query `RadiusOfGyration` [#602](https://github.com/Flowminder/FlowKit/issues/602)

### Changed

-   The `period` argument to `TotalNetworkObjects` in FlowMachine has been renamed `total_by`
-   The `period` argument to `total_network_objects` in FlowClient has been renamed `total_by`
-   The `by` argument to `AggregateNetworkObjects` in FlowMachine has been renamed to `aggregate_by`
-   The `stop_date` argument to the `modal_location_from_dates` and `meaningful_locations_*` functions in FlowClient has been renamed `end_date` [#470](https://github.com/Flowminder/FlowKit/issues/470)
-   `get_result_by_query_id` now accepts a `poll_interval` argument, which allows polling frequency to be changed
-   The `start` and `stop` argument to `EventTableSubset` are now mandatory.
-   `RadiusOfGyration` now returns a `value` column instead of an `rog` column
-   `TotalNetworkObjects` and `AggregateNetworkObjects` now return a `value` column, rather than `statistic_name`
-   All environment variables are now in a single `development_environment` file in the project root, development environment setup has been simplified
-   Default FlowDB users for FlowMachine and FlowAPI have changed from "analyst" and "reporter" to "flowmachine" and "flowapi", respectively
-   Docs and integration tests now use top level compose file
-   The following environment variables have been renamed:
    -   `FLOWMACHINE_SERVER` (FlowAPI) -> `FLOWMACHINE_HOST`
    -   `FM_PASSWORD` (FlowDB), `FLOWDB_PASS` (FlowMachine) -> `FLOWMACHINE_FLOWDB_PASSWORD`
    -   `API_PASSWORD` (FlowDB), `FLOWDB_PASS` (FlowAPI) -> `FLOWAPI_FLOWDB_PASSWORD`
    -   `FM_USER` (FlowDB), `FLOWDB_USER` (FlowMachine) -> `FLOWMACHINE_FLOWDB_USER`
    -   `API_USER` (FlowDB), `FLOWDB_USER` (FlowAPI) -> `FLOWAPI_FLOWDB_USER`
    -   `LOG_LEVEL` (FlowMachine) -> `FLOWMACHINE_LOG_LEVEL`
    -   `LOG_LEVEL` (FlowAPI) -> `FLOWAPI_LOG_LEVEL`
    -   `DEBUG` (FlowDB) -> `FLOWDB_DEBUG`
    -   `DEBUG` (FlowMachine) -> `FLOWMACHINE_SERVER_DEBUG_MODE`
-   The following Docker secrets have been renamed:
    -   `FLOWAPI_DB_USER` -> `FLOWAPI_FLOWDB_USER`
    -   `FLOWAPI_DB_PASS` -> `FLOWAPI_FLOWDB_PASSWORD`
    -   `FLOWMACHINE_DB_USER` -> `FLOWMACHINE_FLOWDB_USER`
    -   `FLOWMACHINE_DB_PASS` -> `FLOWMACHINE_FLOWDB_PASSWORD`
    -   `POSTGRES_PASSWORD_FILE` -> `POSTGRES_PASSWORD`
    -   `REDIS_PASSWORD_FILE` -> `REDIS_PASSWORD`
-   `status` enum in FlowDB renamed to `etl_status`
-   `reset_cache` now requires a redis client argument

### Fixed

-   Fixed being unable to add new users or servers when running FlowAuth with a Postgres database [#622](https://github.com/Flowminder/FlowKit/issues/622)
-   Resetting the cache using `reset_cache` will now reset the state of queries in redis as well [#650](https://github.com/Flowminder/FlowKit/issues/650)
-   Fixed `mode` statistic for `AggregateNetworkObjects` [#651](https://github.com/Flowminder/FlowKit/issues/651)

### Removed

-   Removed `docker-compose-dev.yml`, and docker-compose files in `docs/`, `flowdb/tests/` and `integration_tests/`.
-   Removed `Dockerfile-dev` Dockerfiles
-   Removed `ENV` defaults from the FlowMachine Dockerfile
-   Removed `POSTGRES_DB` environment variable from FlowDB Dockerfile, database name is now hardcoded as `flowdb`

## [0.5.3]

### Added

-   Added new `spatial_aggregate` API endpoint and FlowClient function [#599](https://github.com/Flowminder/FlowKit/issues/599)
-   Added new flowclient API entrypoint, total_network_objects(), to access (with simplified parameters) equivalent flowmachine query [#581](https://github.com/Flowminder/FlowKit/issues/581)
-   Added new flowclient API entrypoint, location_introversion(), to access (with simplified parameters) equivalent flowmachine query [#577](https://github.com/Flowminder/FlowKit/issues/577)
-   Added new flowclient API entrypoint, unique_subscriber_counts(), to access (with simplified parameters) equivalent flowmachine query [#562](https://github.com/Flowminder/FlowKit/issues/562)
-   New schema `aggregates` and table `aggregates.aggregates` have been created for maintaining a record of the process and completion of scheduled aggregates.
-   New `joined_spatial_aggregate` API endpoint and FlowClient function [#600](https://github.com/Flowminder/FlowKit/issues/600)

### Changed

-   `daily_location` and `modal_location` query types are no longer accepted as top-level queries, and must be wrapped using `spatial_aggregate`
-   `JoinedSpatialAggregate` no longer accepts positional arguments
-   `JoinedSpatialAggregate` now supports "avg", "max", "min", "median", "mode", "stddev" and "variance" stats

### Fixed

-   `total_network_objects` no longer returns results from `AggregateNetworkObjects` [#603](https://github.com/Flowminder/FlowKit/issues/603)

## [0.5.2]

### Fixed

-   Fixed [#514](https://github.com/Flowminder/FlowKit/issues/514), which would cause the client to hang after submitting a query that couldn't be created
-   Fixed [#575](https://github.com/Flowminder/FlowKit/issues/575), so that events at midnight are now considered to be happening on the following day

## [0.5.1]

### Added

-   Added `HandsetStats` to FlowMachine.
-   Added new `ContactReferenceLocationStats` query class to FlowMachine.
-   A new zmq message `get_available_dates` was added to the flowmachine server, along with the `/available_dates`
    endpoint in flowapi and the function `get_available_dates()` in flowclient. These allow to determine the dates
    that are available in the database for the supported event types.

### Changed

-   FlowMachine's debugging logs are now from a single logger (`flowmachine.debug`) and include the submodule in the submodule field instead of using it as the logger name
-   FlowMachine's query run logger now uses the logger name `flowmachine.query_run_log`
-   FlowAPI's access, run and debug loggers are now named `flowapi.access`, `flowapi.query` and `flowapi.debug`
-   FlowAPI's access and run loggers, and FlowMachine's query run logger now log to stdout instead of stderr
-   Passwords for Redis and FlowDB must now be explicitly provided to flowmachine via argument to `connect`, env var, or secret

### Removed

-   FlowMachine and FlowAPI no longer support logging to a file

## [0.5.0]

### Added

-   The flowmachine python library is now pip installable (`pip install flowmachine`)
-   The flowmachine server now supports additional actions: `get_available_queries`, `get_query_schemas`, `ping`.
-   Flowdb now contains a new `dfs` schema and associated tables to process mobile money transactions.
    In addition, `flowdb_testdata` contains sample data for DFS transactions.
-   The docs now include three worked examples of CDR analysis using FlowKit.
-   Flowmachine now supports calculating the total amount of various DFS metrics (transaction amount,
    commission, fee, discount) per aggregation unit during a given date range. These metrics are also
    exposed in FlowAPI via the query kind `dfs_metric_total_amount`.

### Changed

-   The JSON structure when setting queries running via flowapi or the flowmachine server has changed:
    query parameters are now "inlined" alongside the `query_kind` key, rather than nested using a separate `params` key.
    Example:
    -   previously: `{"query_kind": "daily_location", "params": {"date": "2016-01-01", "aggregation_unit": "admin3", "method": "last"}}`,
    -   now: `{"query_kind": "daily_location", "date": "2016-01-01", "aggregation_unit": "admin3", "method": "last"}`
-   The JSON structure of zmq reply messages from the flowmachine server was changed.
    Replies now have the form: `{"status": "[success|error]", "msg": "...", "payload": {...}`.
-   The flowmachine server action `get_sql` was renamed to `get_sql_for_query_result`.
-   The parameter `daily_location_method` was renamed to `method`.

## [0.4.3]

### Added

-   When running integration tests locally, normally pytest will automatically spin up servers for flowmachine and flowapi as part of the test setup.
    This can now be disabled by setting the environment variable `FLOWKIT_INTEGRATION_TESTS_DISABLE_AUTOSTART_SERVERS=TRUE`.
-   The integration tests now use the environment variables `FLOWAPI_HOST`, `FLOWAPI_PORT` to determine how to connect to the flowapi server.
-   A new data generator has been added to the synthetic data container which supports more data types, simple disaster simulation, and more plausible behaviours as well as increased performance

### Changed

-   FlowAPI now reports queued/running status for queries instead of just accepted
-   The following environment variables have been renamed:
    -   `DB_USER` -> `FLOWDB_USER`
    -   `DB_USER` -> `FLOWDB_HOST`
    -   `DB_PASS` -> `FLOWDB_PASS`
    -   `DB_PW` -> `FLOWDB_PASS`
    -   `API_DB_USER` -> `FLOWAPI_DB_USER`
    -   `API_DB_PASS` -> `FLOWAPI_DB_PASS`
    -   `FM_DB_USER` -> `FLOWMACHINE_DB_USER`
    -   `FM_DB_PASS` -> `FLOWMACHINE_DB_PASS`
-   Added `numerator_direction` to `ProportionEventType` to allow for proportion of directed events.

### Fixed

-   Server no longer loses track of queries under heavy load
-   `TopUpBalances` no longer always uses entire topups table

### Removed

-   The environment variable `DB_NAME` has been removed.

## [0.4.2]

### Changed

-   `MDSVolume` no longer allows specifying the table, and will always use the `mds` table.
-   All FlowMachine logs are now in structured json form
-   FlowAPI now uses structured logs for debugging messages

## [0.4.1]

### Added

-   Added `TopUpAmount`, `TopUpBalance` query classes to FlowMachine.
-   Added `PerLocationEventStats`, `PerContactEventStats` to FlowMachine

### Removed

-   Removed `TotalSubscriberEvents` from FlowMachine as it is superseded by `EventCount`.

## [0.4.0]

### Added

-   Dockerised development setup, with support for live reload of `flowmachine` and `flowapi` after source code changes.
-   Pre-commit hook for Python formatting with black.
-   Added new `IntereventPeriod`, `ContactReciprocal`, `ProportionContactReciprocal`, `ProportionEventReciprocal`, `ProportionEventType` and `MDSVolume` query classes to FlowMachine.

### Changed

-   `CustomQuery` now requires column names to be specified
-   Query classes are now required to declare the column names they return via the `column_names` property
-   FlowAPI now reports whether a query is queued or running when polling
-   FlowDB test data and synthetic data images are now available from their own Docker repos (Flowminder/flowdb-testdata, Flowminder/flowdb-synthetic-data)
-   Changed query class name from `NocturnalCalls` to `NocturnalEvents`.

### Fixed

-   FlowAPI is now an installable python module

### Removed

-   Query objects can no longer be recalculated to cache and must be explicitly removed first
-   Arbitrary `Flow` maths
-   `EdgeList` query type
-   Removes query class `ProportionOutgoing` as it becomes redundant with the the introduction of `ProportionEventType`.

## [0.3.0]

### Added

-   API route for retrieving geography data from FlowDB
-   Aggregated meaningful locations are now available via FlowAPI
-   Origin-destination matrices between meaningful locations are now available via FlowAPI
-   Added new `MeaningfulLocations`, `MeaningfulLocationsAggregate` and `MeaningfulLocationsOD` query classes to FlowMachine

### Changed

-   Constructors for `HartiganCluster`, `LabelEventScore`, `EventScore` and `CallDays` now have different signatures
-   Restructured and extended documentation; added high-level overview and more targeted information for different types of users

## [0.2.2]

### Added

-   Support for running FlowDB as an arbitrary user via docker's `--user` flag

### Removed

-   Support for setting the uid and gid of the postgres user when building FlowDB

## [0.2.1]

### Fixed

-   Fixed being unable to build if the port used by `git://` is not open

## [0.2.0]

### Added

-   Added utilities for managing and inspecting the query cache

## [0.1.2]

### Changed

-   FlowDB now requires a password to be set for the flowdb superuser

## [0.1.1]

### Added

-   Support for password protected redis

### Changed

-   Changed the default redis image to bitnami's redis (to enable password protection)

## [0.1.0]

### Added

-   Added structured logging of access attempts, query running, and data access
-   Added CHANGELOG.md
-   Added support for Postgres JIT in FlowDB
-   Added total location events metric to FlowAPI and FlowClient
-   Added ETL bookkeeping schema to FlowDB

### Changed

-   Added changelog update to PR template
-   Increased default shared memory size for FlowDB containers

### Fixed

-   Fixed being unable to delete groups in FlowAuth
-   Fixed `make up` not working with defaults

## [0.0.5]

### Added

-   Added Python 3.6 support for FlowClient


[unreleased]: https://github.com/Flowminder/FlowKit/compare/1.17.0...master
[1.17.0]: https://github.com/Flowminder/FlowKit/compare/1.16.0...1.17.0
[1.16.0]: https://github.com/Flowminder/FlowKit/compare/1.15.0...1.16.0
[1.15.0]: https://github.com/Flowminder/FlowKit/compare/1.14.6...1.15.0
[1.14.6]: https://github.com/Flowminder/FlowKit/compare/1.14.5...1.14.6
[1.14.5]: https://github.com/Flowminder/FlowKit/compare/1.14.4...1.14.5
[1.14.4]: https://github.com/Flowminder/FlowKit/compare/1.14.3...1.14.4
[1.14.3]: https://github.com/Flowminder/FlowKit/compare/1.14.2...1.14.3
[1.14.2]: https://github.com/Flowminder/FlowKit/compare/1.14.1...1.14.2
[1.14.1]: https://github.com/Flowminder/FlowKit/compare/1.14.0...1.14.1
[1.14.0]: https://github.com/Flowminder/FlowKit/compare/1.13.0...1.14.0
[1.13.0]: https://github.com/Flowminder/FlowKit/compare/1.12.1...1.13.0
[1.12.0]: https://github.com/Flowminder/FlowKit/compare/1.11.1...1.12.0
[1.11.1]: https://github.com/Flowminder/FlowKit/compare/1.11.0...1.11.1
[1.11.0]: https://github.com/Flowminder/FlowKit/compare/1.10.0...1.11.0
[1.10.0]: https://github.com/Flowminder/FlowKit/compare/1.9.4...1.10.0
[1.9.4]: https://github.com/Flowminder/FlowKit/compare/1.9.3...1.9.4
[1.9.3]: https://github.com/Flowminder/FlowKit/compare/1.9.2...1.9.3
[1.9.2]: https://github.com/Flowminder/FlowKit/compare/1.9.1...1.9.2
[1.9.1]: https://github.com/Flowminder/FlowKit/compare/1.9.0...1.9.1
[1.9.0]: https://github.com/Flowminder/FlowKit/compare/1.8.0...1.9.0
[1.8.0]: https://github.com/Flowminder/FlowKit/compare/1.7.0...1.8.0
[1.7.0]: https://github.com/Flowminder/FlowKit/compare/1.6.1...1.7.0
[1.6.1]: https://github.com/Flowminder/FlowKit/compare/1.6.0...1.6.1
[1.6.0]: https://github.com/Flowminder/FlowKit/compare/1.5.1...1.6.0
[1.5.1]: https://github.com/Flowminder/FlowKit/compare/1.5.0...1.5.1
[1.5.0]: https://github.com/Flowminder/FlowKit/compare/1.4.0...1.5.0
[1.4.0]: https://github.com/Flowminder/FlowKit/compare/1.3.3...1.4.0
[1.3.3]: https://github.com/Flowminder/FlowKit/compare/1.3.2...1.3.3
[1.3.2]: https://github.com/Flowminder/FlowKit/compare/1.3.1...1.3.2
[1.3.1]: https://github.com/Flowminder/FlowKit/compare/1.3.0...1.3.1
[1.3.0]: https://github.com/Flowminder/FlowKit/compare/1.2.1...1.3.0
[1.2.1]: https://github.com/Flowminder/FlowKit/compare/1.2.0...1.2.1
[1.2.0]: https://github.com/Flowminder/FlowKit/compare/1.1.1...1.2.0
[1.1.1]: https://github.com/Flowminder/FlowKit/compare/1.1.0...1.1.1
[1.1.0]: https://github.com/Flowminder/FlowKit/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/Flowminder/FlowKit/compare/0.9.1...1.0.0
[0.9.1]: https://github.com/Flowminder/FlowKit/compare/0.9.0...0.9.1
[0.9.0]: https://github.com/Flowminder/FlowKit/compare/0.8.0...0.9.0
[0.8.0]: https://github.com/Flowminder/FlowKit/compare/0.7.0...0.8.0
[0.7.0]: https://github.com/Flowminder/FlowKit/compare/0.6.4...0.7.0
[0.6.4]: https://github.com/Flowminder/FlowKit/compare/0.6.3...0.6.4
[0.6.3]: https://github.com/Flowminder/FlowKit/compare/0.6.2...0.6.3
[0.6.2]: https://github.com/Flowminder/FlowKit/compare/0.6.1...0.6.2
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
