Title: Managing users and access

## Managing FlowAPI access with FlowAuth

### Granting user permissions 

FlowAuth is the tool which analysts will use to generate tokens which will allow them to communicate with a FlowKit server through FlowAPI. The following steps using the FlowAuth administration tool are required to add a user and allow them to generate access tokens:

1. Log into FlowAuth as an administrator.

2. Under "Servers", add a new server by clicking the '+' button, uploading the spec downloaded from the server, and setting the latest expiry and longest life for tokens.

4. Under "Users", add a new user, and set the username and password. Leave 'Roles' blank for now.

5. Under "Roles", create a new role under your sever and provide appropriate scopes and lifetimes. Associate the role with the user under 'Members'.


The user can then log into FlowAuth and generate a token (see the [analyst section](../../analyst/index.md#flowauth) for instructions).

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
