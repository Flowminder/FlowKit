Title: Managing users and access

## Managing FlowAPI access with FlowAuth

### Granting user permissions 

FlowAuth is the tool which analysts will use to generate tokens which will allow them to communicate with a FlowKit server through FlowAPI. The following steps using the FlowAuth administration tool are required to add a user and allow them to generate access tokens:

1. Log into FlowAuth as an administrator.

2. Under "Servers", add a new server by clicking the '+' button, uploading the spec downloaded from the server, and setting the latest expiry and longest life for tokens.

5. Create a new role for the server

    1. Under "Roles", add a new role to your server. An analyst role should have `get_available_dates`, `get_results` and `run` scopes enabled, along with appropriate geographic roles.
  
    2. You only need to do this once per server.

4. Under "Users", add a new user, set the username and password and assign them the new role.


The user can then log into FlowAuth and generate a token (see the [analyst section](../../analyst/index.md#flowauth) for instructions).

### Advanced role management

Each `role` consists of a set of `scopes`, derived from the server's spec. Scopes grant roles the following capabilities;

 - `run`: A role with the `run` scope can request runs of queries that it has the scopes for.
 - `get_results`: A role with the `get_results` scope can retrieve results for completed Flowkit queries
 - `get_available_dates`: This scopes permits the role holder to retrieve the range of dates available on the server.
All other scopes are for specific flowmachine queries, grouped first by geographic aggregation, then by main query and finally by sub-query within those main queries. It is anticipated that users will mainly be restricting role by geography. Each of these scopes grant the capability to poll progress of that query and, in conjunction with the `run` and `get_results` roles, run and return results from those queries.

Some example advanced roles could be:

| Name | Purpose | Scopes |
| ---- | ------- | ------ |
| State-level analyst | A role for analysts who can perform any analysis, but only at state-level scales | `run`, `get_available_dates`, `get_results`, `admin1` and `nonspatial` |
| Automated monitor | A role for an automated user that triggers a "flows" on a timer, but will never need to read it | `get_available_dates`, `run`, `admin3:flows` |
| Viewer | A role for any user that can view the results of any query, but not run new ones. | `get_results`, `get_available_dates`, all query scopes. |

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
