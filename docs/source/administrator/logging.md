## Audit Trails

FlowKit supports structured logging in JSON form for all interactions with FlowAPI, primarily to facilitate audit trails.

Three kinds of access log are written on each request handled by FlowAPI: authentication, data access at API side, and data access on the FlowMachine side.


### Authentication Logs

FlowAPI logs all access attempts whether successful or not to stdout, and to a rotating log file. By default, this will be written at `/var/log/flowapi/flowkit-access.log`. The directory the log files are written to can be changed by setting the `LOG_DIRECTORY` environment variable.

Where authentication succeeds, the log message will have a `level` field of `info`, and an `event` type of `AUTHENTICATED`:

```json
{"request_id": "fe1d5dd2-ddfb-4b34-9d1e-4ebfc205d64c", "route": "/api/0/run", "user": "TEST_USER", "src_ip": "127.0.0.1", "json_payload": {"params": {"date": "2016-01-01", "level": "admin3", "daily_location_method": "last", "aggregation_unit": "admin3", "subscriber_subset": "all"}, "query_kind": "daily_location"}, "event": "AUTHENTICATED", "logger": "flowkit-access", "level": "info", "timestamp": "2019-01-10T13:57:35.262214Z"}
```

In general, access log messages contain at a minimum the route that access was requested to, any json payload, source IP address for the request, and a timestamp. Every request _also_ has a _unique id_, which will be the same across all log entries related to that request.

If authentication fails, then the reason is also included in the log message, along with any error message and as much information about the request and requester as can be discerned.

### API Usage Logs

After a request is successfully authenticated (has a valid token), the _nature_ of the request will be logged at several points. When the request is received, if at any point the request is rejected because the provided token did not grant access, and when the request is fulfilled.

As with the authentication log, the usage log is written to both stdout and a rotating log file. Logs will be written to `query-runs.log` in the same directory as the authentication log.

### FlowMachine Usage Logs

If a request has triggered an action in the FlowMachine backend, logs will also be written there. These logs will include the `request_id` for the API request which originally triggered them.

As with the FlowAPI loggers, these messages are written both to stdout and a rotating log file (`/var/log/flowmachine-server/query-runs.log`, although the directory can be set using the `LOG_DIRECTORY` environment variable.)

### Complete Logging Cycle

A complete logging cycle for a successful request to retrieve a previously run query's results might look like this:

FlowAPI access log:

```json
{"request_id": "d2892489-8fb8-40ec-94e6-2467266a0226", "route": "/api/0/get/ddc61a04f608dee16fff0655f91c2057", "user": "TEST_USER", "src_ip": "127.0.0.1", "json_payload": null, "event": "AUTHENTICATED", "logger": "flowkit-access", "level": "info", "timestamp": "2019-01-10T14:11:03.331967Z"}
```

FlowAPI usage log:

```json
{"request_id": "d2892489-8fb8-40ec-94e6-2467266a0226", "query_kind": "DAILY_LOCATION", "route": "/api/0/get/ddc61a04f608dee16fff0655f91c2057", "user": "TEST_USER", "src_ip": "127.0.0.1", "json_payload": null, "query_id": "ddc61a04f608dee16fff0655f91c2057", "claims": {"permissions": {"get_result": true, "poll": true, "run": true}, "spatial_aggregation": ["admin2", "admin0", "admin3", "admin1", "cell", "site"]}, "event": "Received", "logger": "flowkit-query", "level": "info", "timestamp": "2019-01-10T14:11:03.337052Z"}
{"request_id": "d2892489-8fb8-40ec-94e6-2467266a0226", "query_kind": "DAILY_LOCATION", "route": "/api/0/get/ddc61a04f608dee16fff0655f91c2057", "user": "TEST_USER", "src_ip": "127.0.0.1", "json_payload": null, "query_id": "ddc61a04f608dee16fff0655f91c2057", "claims": {"permissions": {"get_result": true, "poll": true, "run": true}, "spatial_aggregation": ["admin2", "admin0", "admin3", "admin1", "cell", "site"]}, "event": "Authorised", "logger": "flowkit-query", "level": "info", "timestamp": "2019-01-10T14:11:03.341010Z"}
```

FlowMachine usage log:

```json
{"query_id": "ddc61a04f608dee16fff0655f91c2057", "query_kind": "daily_location", "message": "b'{\"request_id\":\"d2892489-8fb8-40ec-94e6-2467266a0226\",\"action\":\"get_query_kind\",\"query_id\":\"ddc61a04f608dee16fff0655f91c2057\"}'", "request_id": "d2892489-8fb8-40ec-94e6-2467266a0226", "params": {"query_id": "ddc61a04f608dee16fff0655f91c2057"}, "event": "get_query_kind", "logger": "flowmachine-server", "level": "info", "timestamp": "2019-01-10T14:11:03.335437Z"}
{"query_id": "ddc61a04f608dee16fff0655f91c2057", "retrieved_params": {"aggregation_unit": "admin3", "daily_location_method": "last", "date": "2016-01-01", "level": "admin3", "subscriber_subset": "all"}, "message": "b'{\"request_id\":\"d2892489-8fb8-40ec-94e6-2467266a0226\",\"action\":\"get_params\",\"query_id\":\"ddc61a04f608dee16fff0655f91c2057\"}'", "request_id": "d2892489-8fb8-40ec-94e6-2467266a0226", "params": {"query_id": "ddc61a04f608dee16fff0655f91c2057"}, "event": "get_params", "logger": "flowmachine-server", "level": "info", "timestamp": "2019-01-10T14:11:03.339602Z"}
{"query_id": "ddc61a04f608dee16fff0655f91c2057", "message": "b'{\"request_id\":\"d2892489-8fb8-40ec-94e6-2467266a0226\",\"action\":\"get_sql\",\"query_id\":\"ddc61a04f608dee16fff0655f91c2057\"}'", "request_id": "d2892489-8fb8-40ec-94e6-2467266a0226", "params": {"query_id": "ddc61a04f608dee16fff0655f91c2057"}, "event": "get_sql", "logger": "flowmachine-server", "level": "info", "timestamp": "2019-01-10T14:11:03.358644Z"}
```

Note that the `request_id` field is identical across the five log entries, which lets you match the request across the multiple services.

### Managing and Monitoring Logs

Because FlowKit employs structured logging, and all log messages are JSON objects, the access and usage logs are easy to use with tools like [Logstash](https://www.elastic.co/products/logstash). Getting FlowKit logs into Logstash is as easy as configuring it to consume the log files generated.

Once approach to this is to mount the volumes for `/var/log/flowmachine-server/` and `/var/log/flowapi/` and expose the log files to Logstash, or a log shipper.

Alternatively, [Filebeat](https://www.elastic.co/docker-kubernetes-container-monitoring) allows you to integrate the logs from stdout directly into your monitoring system.

   