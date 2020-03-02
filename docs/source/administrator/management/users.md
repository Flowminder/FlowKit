Title: Managing users and access

## Managing FlowAPI access with FlowAuth

### Granting user permissions 

FlowAuth is the tool which analysts will use to generate tokens which will allow them to communicate with a FlowKit server through FlowAPI. The following steps using the FlowAuth administration tool are required to add a user and allow them to generate access tokens:

1. Log into FlowAuth as an administrator.

2. Under "API Routes", add any applicable API routes (e.g. `daily_location`).

3. Under "Aggregation Units", add any applicable aggregation units (e.g. `admin3`).

3. Under "Servers", add a new server and give it a name. Note that the name must match the `FLOWAPI_IDENTIFIER` variable set in the FlowAPI docker container on this server.

4. Enable any permissions for this server under "API Permissions", and aggregation units under "Aggregation Units".

5. Under "Users", add a new user, and set the username and password.

6. Either:
    - Add a server to the user, and enable/disable API permissions and aggregation units,
    <p>
7. Or:
    <p>
    - Under "Groups", add a new group,

    - Add a server to the group, and enable/disable API permissions and aggregation units,

    - Add the user to the group.

The user can then log into FlowAuth and generate a token (see the [analyst section](../analyst/index.md#flowauth) for instructions).

## Managing access to FlowDB

Because FlowDB is built using [PostgreSQL](https://postgresql.org), you can use standard Postgres commands to [manage users](https://www.postgresql.org/docs/current/sql-createrole.html). FlowDB contains some default roles which you can use as group templates using `CREATE ROLE INHERIT`:

| Schema | Read access | Write access |
| ------ | ----------- | ------------ |
| cache | flowmachine, flowapi | flowmachine |
| results | flowmachine | flowmachine |
| features | flowmachine | flowmachine |
| geography | flowmachine, flowapi | |
| population | flowmachine | |
| elevation | flowmachine | |
| events | flowmachine | |
| dfs | flowmachine | |
| infrastructure | flowmachine | |
| routing | flowmachine | |
| interactions | flowmachine | |
| etl | flowmachine | |

It is recommended that after creating a user with a temporary password that they connect using psql, and use the `\password` command to set a new password.

!!!note

    You can manage FlowDB using psql from inside the docker container:
    
    ```bash
    docker exec -it <container_name> psql -U flowdb
    ```