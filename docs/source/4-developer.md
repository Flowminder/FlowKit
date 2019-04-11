Title: Developer

# Information for Developers

Because FlowKit deployment is primarily done using Docker, the installation for developers is slightly different, see the instructions [here](2-install.md). 

An outline roadmap is provided below together with details about [contributing to the project](#contrib).

Followed by a guide to the [FlowAPI](#flowapi). 

FlowMachine specifications are found [here](#flowmachine).

FlowDB details are found [here](#flowdb).

<a href="roadmap"></a>

## Roadmap

### Now

-   More secure method for linking FlowAuth and FlowMachine
-   Additional home location aggregate
-   Work locations aggregate
-   Benchmarking
-   Cache management
-   Audit logs
-   Support for downloading geographies via API

### Next

-   Additional FlowMachine aggregates exposed via API
-   FlowMachine library release
-   Additional language targets for FlowClient
-   Expanded worked examples
-   Two-factor authentication support for FlowAuth
-   Alternative login provider support for FlowAuth
-   Interactive API specification
-   Custom geography support

### Later

-   New metrics and insights
-   Plugin support
-   Non-spatial aggregations
-   Enhanced temporal aggregations
-   Individual level API
-   Data source input/output connectors

<a name="contrib"></a>
<p>
<p>

### Contributing

We are creating FlowKit at [Flowminder](http://flowminder.org).

#### Get involved
You are welcome to contribute to the FlowKit library. To get started:  

1. Check [Issues](https://github.com/Flowminder/FlowKit/issues) to see what we are working on right now.  
2. Express your interest in a particular [issue](https://github.com/Flowminder/FlowKit/issues) by submitting a comment, or submit your own [issue](https://github.com/Flowminder/FlowKit/issues).
3. We will get back to you about working together.

#### Code contributions
If you plan to make a major contribution, please create a [pull request](https://github.com/Flowminder/FlowKit/pulls) with the feature or bug fix.


<a name="flowapi"></a>

## FlowAPI Guide


This section describes the FlowAPI routes.

FlowAPI is an HTTP API which provides access to the functionality of [FlowMachine](#flowmachine).

FlowAPI uses [ZeroMQ](http://zeromq.org/) for asynchronous communication with the FlowMachine server.

### API Routes

The API exposes four routes:

- `/run`: set a query running in FlowMachine.

- `/poll/<query_id>`: get the status of a query.

- `/get/<query_id>`: return the result of a finished query.

- `/geography/<aggregation_unit>`: return geography data for a given aggregation unit.

- `/available_dates` : return the dates available to calculate metrics over

At present, the following query types are accessible through FlowAPI:

- `daily_location`

    A statistic representing where subscribers are on a given day. Must be used in combination with `modal_location`, `flows` or `spatial_aggregate`.

- `modal_location`

    The mode of a set of daily locations. Must be used in combination with `flows` or `spatial_aggregate`.

- `spatial_aggregate`
    
    A spatial aggregate of `modal_location` or `daily_location`, suitable for return via the API.

- `flows`

    The difference in locations between two location queries.

- `location_event_counts`

    Count of total interactions in a time period, aggregated to a spatial unit.

- `meaningful_locations_aggregate`

    Count of "meaningful" locations for individual subscribers (for example, home and work)
    based on a clustering of the cell towers they use and their usage patterns for those towers,
    aggregated to a spatial unit.

- `meaningful_locations_od_matrix`

    OD matrix between two individual-level "meaningful" locations (see above), aggregated to a spatial unit.

- `total_network_objects`
    
    Count of mobile phone cells per area active based on CDR traffic within a time period, broken down into time buckets.


### FlowAPI Access tokens

As explained in the [quick install guide](2-install.md), user authentication and access control are handled through the use of [JSON Web Tokens (JWT)](http://jwt.io). There are two categories of permissions which can be granted to a user:

- API route permissions

    API routes (`run`, `poll`, `get_result`) the user is allowed to access.

- Spatial aggregation unit permissions

    Level of spatial aggregation at which the user is allowed to access the results of queries. Currently supports administrative levels `admin0`, `admin1`, `admin2`, `admin3`.

JWTs allow these access permissions to be granted independently for each query kind (e.g. `daily_location`, `modal_location`). The [FlowAuth](2-install.md#installing-flowauth) authentication management system is designed to generate JWTs for accessing FlowAPI.


<a name="flowmachine">

## FlowMachine

FlowMachine is a Python toolkit for the analysis of CDR data. It is essentially a python wrapper for postgres SQL queries, including geographical queries with postgis. All of the classes in flowmachine define an SQL query under the hood.

### Documentation

Documentation for FlowMachine can be found [here](../flowmachine/flowmachine/). A worked example of using FlowMachine for analysis is provided [here](../worked_examples/mobile-data-usage/).


<a name="flowdb">

## FlowDB

FlowDB is database designed for storing, serving, and analysing mobile operator data. The project's main features are:

-   Uses standard schema for most common mobile operator data
-   Is built as a [Docker](http://docker.com/) container
-   Uses [PostgreSQL 11](https://www.postgresql.org/docs/11/static/release-11.html)
-   Grants different permissions for users depending on need
-   Is configured for high-performance operations
-   Comes with
    -   [PostGIS](https://postgis.net) for geospatial data handling and analysis
    -   [pgRouting](http://pgrouting.org) for advanced spatial analysis
    -   Utilities for connecting with Oracle databases ([oracle_fdw](https://github.com/laurenz/oracle_fdw))
    -   Scheduled tasks run in database ([pg_cron](https://github.com/citusdata/pg_cron))

### Synthetic Data

In addition to the bare FlowDB container and the test data container used for tests a 'synthetic' data container is available. This container generates arbitrary quantities of data at runtime.

Two data generators are available - a Python based generator, which supports reproducible random data, and an SQL based generator which supports greater data volumes, more data types, plausible subscriber behaviour, and simple disaster scenarios.

Both are packaged in the `flowminder/flowdb-synthetic-data` docker image, and configured via environment variables.

For example, to generate a repeatable random data set with seven days of data, where 20,000 subscribers make 10,000 calls each day and use 5,000 cells:

```bash
docker run --name flowdb_synth_data -e FLOWMACHINE_FLOWDB_PASS=foo -e FLOWAPI_FLOWDB_PASS=foo \
 --publish 9000:5432 \
 -e N_CALLS=10000 -e N_SUBSCRIBERS=20000 -e N_CELLS=5000 -e N_DAYS=7 -e SYNTHETIC_DATA_GENERATOR=python \
 -e SUBSCRIBERS_SEED=11111 -e CALLS_SEED=22222 -e CELLS_SEED=33333 \
 --detach flowminder/flowdb-testdata:latest
```

Or to generate an equivalent data set which includes TACs, mobile data sessions and sms:

```bash
docker run --name flowdb_synth_data -e FLOWMACHINE_FLOWDB_PASS=foo -e FLOWAPI_FLOWDB_PASS=foo \
 --publish 9000:5432 \
 -e N_CALLS=10000 -e N_SUBSCRIBERS=20000 -e N_CELLS=5000 -e N_SITES=5000 -e N_DAYS=7 -e SYNTHETIC_DATA_GENERATOR=sql \
 -e N_SMS=10000 -e N_MDS=10000 \
 --detach flowminder/flowdb-testdata:latest
```

!!! warning
    For generating large datasets, it is recommended that you use the SQL based generator. 
    
##### SQL Generator features

The SQL generator supports semi-plausible behaviour - each subscriber has a 'home' region, and will typically (by default, 95% of the time) call/sms/use data from cells in that region. Subscribers will occasionally (by default, 1% chance per day) relocate to a new home region.
Subscribers also have a consistent phone model across time, and a consistent set of other subscribers who they interact with (by default, `5*N_SUBSCRIBERS` calling pairs are used).

Mass relocation scenarios are also supported - a designated admin 2 region can be chosen to be 'off limits' to all subscribers for a period. Any subscribers ordinarily resident will relocate to another randomly chosen region, and no subscriber will call from a cell within the region or relocate there while the region is off limits.

##### Parameters

- `N_DAYS`: number of days of data to generate, defaults to 7
- `N_SUBSCRIBERS`: number of simulated subscribers, defaults to 4,000 
- `N_TACS`: number of mobile phone models, defaults to 1,000 (SQL generator only)
- `N_SITES`: number of mobile sites, defaults to 1,000 (SQL generator only)
- `N_CELLS`: number of cells, defaults to 1,000
- `N_CALLS`: number of calls to generate per day, defaults to 200,000
- `N_SMS`: number of sms to generate per day, defaults to 200,000 (SQL generator only)
- `N_MDS`: number of mobile data sessions to generate per day, defaults to 200,000 (SQL generator only)
- `SUBSCRIBERS_SEED`: random seed used when generating subscribers, defaults to 11111 (Python generator only)
- `CALLS_SEED`: random seed used when generating calls, defaults to 22222 (Python generator only)
- `CELLS_SEED`: random seed used when generating cells, defaults to 33333 (Python generator only)
- `SYNTHETIC_DATA_GENERATOR`: which data generator to use, may be`'sql'` or `'python'`. Defaults to `'sql'`
- `P_OUT_OF_AREA`: probability that an event is taking place out of a subscriber's home region. Defaults to 0.05
- `P_RELOCATE`: probability that each subscriber relocates each day, defaults to 0.01
- `INTERACTIONS_MULTIPLIER`: multiplier for interaction pairs, defaults to 5.

### Caveats

#### Shared Memory

You will typically need to increase the default shared memory available to docker containers when running FlowDB. You can do this either by setting `shm_size` for the FlowDB container in your compose or stack file, or by passing the `--shm-size` argument to the `docker run` command.

#### Bind Mounts and User Permissions

By default, FlowDB will create and attach a docker volume that contains all data. In some cases, this will be sufficient for use.

However, you will often wish to set up bind mounts to hold the data and allow FlowDB to consume new data. To avoid sticky situations with permissions, you will want to specify the uid and gid that FlowDB runs with to match an existing user on the host system.

Adding a bind mount using `docker-compose` is simple:

```yaml
services:
    flowdb:
    ...
        user: HOST_USER_ID:HOST_GROUP_ID
        volumes:
          - /path/to/store/data/on/host:/var/lib/postgresql/data
          - /path/to/consume/data/from/host:/etl:ro
```

This creates two bind mounts, the first is FlowDB's internal storage, and the second is a *read only* mount for loading new data. The user FlowDB runs as inside the container will also be changed to the uid specified. 

!!! warning
    If the bind mounted directories do not exist, docker will create them and you will need to `chown` them to the correct user.

And similarly when using `docker run`:

```bash
docker run --name flowdb_testdata -e FLOWMACHINE_FLOWDB_PASS=foo -e FLOWAPI_FLOWDB_PASS=foo \
 --publish 9000:5432 \
 --user HOST_USER_ID:HOST_GROUP_ID \
 -v /path/to/store/data/on/host:/var/lib/postgresql/data \
 -v /path/to/consume/data/from/host:/etl:ro \
 --detach flowminder/flowdb-testdata:latest
```

!!! tip
    To run as the current user, you can simply replace `HOST_USER_ID:HOST_GROUP_ID` with `$(id -u):$(id -g)`.
 
 
!!! warning
    Using the `--user` flag without a bind mount specified will not work, and you will see an error
    like this: `initdb: could not change permissions of directory "/var/lib/postgresql/data": Operation not permitted`.
    
    When using docker volumes, docker will manage the permissions for you.


## FlowAuth

## Quick setup to run the Frontend tests interactively

For development purposes, it is useful to be able to run the Flowauth frontend tests interactively.

- As an initial step, ensure that all the relevant Python and Javascript dependencies are installed.
```
cd /path/to/flowkit/flowauth/
pipenv install

cd /path/to/flowkit/flowauth/frontend
npm install
```

- The following command sets both the flowauth backend and frontend running (and also opens the flowauth web interface at `http://localhost:3000/` in the browser).
```
cd /path/to/flowkit/flowauth/
pipenv run start-all
```

- To open the Cypress UI, run the following in a separate terminal session:
```
cd /path/to/flowkit/flowauth/frontend/
npm run cy:open
```

- You can then click the button "Run all specs", or select an individual spec to run only a subset of the tests.
