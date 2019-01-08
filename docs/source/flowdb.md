# FlowDB

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


## Caveats

You will typically need to increase the default shared memory available to docker containers when running FlowDB. You can do this either by setting `shm_size` for the FlowDB container in your compose or stack file, or by passing the `--shm-size` argument to the `docker run` command.  