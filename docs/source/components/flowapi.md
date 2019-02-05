# FlowAPI

FlowAPI is an HTTP API which provides access to the functionality of [FlowMachine](../flowmachine/).

FlowAPI uses [ZeroMQ](http://zeromq.org/) for asynchronous communication with the FlowMachine server.

## API Routes

The API exposes four routes:

- `/run`: set a query running in FlowMachine.

- `/poll/<query_id>`: get the status of a query.

- `/get/<query_id>`: return the result of a finished query.

- `/geography/<aggregation_unit>`: return geography data for a given aggregation unit.

At present, four query kinds are accessible through FlowAPI:

- `daily_location`

    A statistic representing where subscribers are on a given day, aggregated to a spatial location level.

- `modal_location`

    The mode of a set of daily locations.

- `flows`

    The difference in locations between two location queries.

- `location_event_counts`

    Count of total interactions in a time period, aggregated to a spatial location level.

## Access tokens

User authentication and access control are handled through the use of [JSON Web Tokens (JWT)](http://jwt.io). There are two categories of permissions which can be granted to a user:

- API route permissions

    API routes (`run`, `poll`, `get_result`) the user is allowed to access.

- Spatial aggregation unit permissions

    Level of spatial aggregation at which the user is allowed to access the results of queries. Currently supports administrative levels `admin0`, `admin1`, `admin2`, `admin3`.

JWTs allow these access permissions to be granted independently for each query kind (e.g. `daily_location`, `modal_location`). The [FlowAuth](../flowauth/) authentication management system is designed to generate JWTs for accessing FlowAPI.
