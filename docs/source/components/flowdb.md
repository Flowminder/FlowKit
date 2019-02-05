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

### Shared Memory

You will typically need to increase the default shared memory available to docker containers when running FlowDB. You can do this either by setting `shm_size` for the FlowDB container in your compose or stack file, or by passing the `--shm-size` argument to the `docker run` command.

### Bind Mounts and User Permissions

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
docker run --name flowdb_testdata -e FM_PASSWORD=foo -e API_PASSWORD=foo \
 --publish 9000:5432 \
 --user HOST_USER_ID:HOST_GROUP_ID \
 -v /path/to/store/data/on/host:/var/lib/postgresql/data \
 -v /path/to/consume/data/from/host:/etl:ro \
 --detach flowminder/flowdb:latest
```

!!! tip
    To run as the current user, you can simply replace `HOST_USER_ID:HOST_GROUP_ID` with `$(id -u):$(id -g)`.
 
 
!!! warning
    Using the `--user` flag without a bind mount specified will not work, and you will see an error
    like this: `initdb: could not change permissions of directory "/var/lib/postgresql/data": Operation not permitted`.
    
    When using docker volumes, docker will manage the permissions for you.