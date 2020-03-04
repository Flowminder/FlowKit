Title: Logging and audit trails

FlowKit supports structured logging in JSON form throughout FlowMachine and FlowAPI. By default both FlowMachine and FlowAPI will only log errors, and audit logs.

Audit trail logs are _always_ written to stdout. Error and debugging logs are, by default, only written to stderr. 

### Audit trail logs

Three kinds of access log are written on each request handled by FlowAPI: authentication, data access at API side, and data access on the FlowMachine side.


#### Authentication Logs

FlowAPI logs all access attempts whether successful or not to stdout using a logger named `flowapi.access`.

Where authentication succeeds, the log message will have a `level` field of `info`, and an `event` type of `AUTHENTICATED`:

```json
{
	"request_id": "fe1d5dd2-ddfb-4b34-9d1e-4ebfc205d64c",
	"route": "/api/0/run",
	"user": "TEST_USER",
	"src_ip": "127.0.0.1",
	"json_payload": {
		"params": {
			"date": "2016-01-01",
			"level": "admin3",
			"method": "last",
			"aggregation_unit": "admin3",
			"subscriber_subset": "all"
		},
		"query_kind": "daily_location"
	},
	"event": "AUTHENTICATED",
	"logger": "flowapi.access",
	"level": "info",
	"timestamp": "2019-01-10T13:57:35.262214Z"
}
```

In general, access log messages contain at a minimum the route that access was requested to, any json payload, source IP address for the request, and a timestamp. Every request _also_ has a _unique id_, which will be the same across all log entries related to that request.

If authentication fails, then the reason is also included in the log message, along with any error message and as much information about the request and requester as can be discerned.

#### API Usage Logs

After a request is successfully authenticated (has a valid token), the _nature_ of the request will be logged at several points. When the request is received, if at any point the request is rejected because the provided token did not grant access, and when the request is fulfilled.

As with the authentication log, the usage log is written to stdout using a logger named `flowapi.query`.

#### FlowMachine Usage Logs

If a request has triggered an action in the FlowMachine backend, logs will also be written there. These logs will include the `request_id` for the API request which originally triggered them.

As with the FlowAPI loggers, these messages are written to stdout, using the `flowmachine.query_run_log` logger.

#### Complete Logging Cycle

A complete logging cycle for a successful request to retrieve a previously run query's results might look like this:

FlowAPI access log:

```json
{
	"request_id": "d2892489-8fb8-40ec-94e6-2467266a0226",
	"route": "/api/0/get/ddc61a04f608dee16fff0655f91c2057",
	"user": "TEST_USER",
	"src_ip": "127.0.0.1",
	"json_payload": null,
	"event": "AUTHENTICATED",
	"logger": "flowapi.access",
	"level": "info",
	"timestamp": "2019-01-10T14:11:03.331967Z"
}
```

FlowAPI usage log:

```json
{
	"request_id": "d2892489-8fb8-40ec-94e6-2467266a0226",
	"query_kind": "DAILY_LOCATION",
	"route": "/api/0/get/ddc61a04f608dee16fff0655f91c2057",
	"user": "TEST_USER",
	"src_ip": "127.0.0.1",
	"json_payload": null,
	"query_id": "ddc61a04f608dee16fff0655f91c2057",
	"claims": {
		"permissions": { "get_result": true, "poll": true, "run": true },
		"spatial_aggregation": [
			"admin2",
			"admin0",
			"admin3",
			"admin1",
			"cell",
			"site"
		]
	},
	"event": "Received",
	"logger": "flowapi.query",
	"level": "info",
	"timestamp": "2019-01-10T14:11:03.337052Z"
}

{
	"request_id": "d2892489-8fb8-40ec-94e6-2467266a0226",
	"query_kind": "DAILY_LOCATION",
	"route": "/api/0/get/ddc61a04f608dee16fff0655f91c2057",
	"user": "TEST_USER",
	"src_ip": "127.0.0.1",
	"json_payload": null,
	"query_id": "ddc61a04f608dee16fff0655f91c2057",
	"claims": {
		"permissions": { "get_result": true, "poll": true, "run": true },
		"spatial_aggregation": [
			"admin2",
			"admin0",
			"admin3",
			"admin1",
			"cell",
			"site"
		]
	},
	"event": "Authorised",
	"logger": "flowapi.query",
	"level": "info",
	"timestamp": "2019-01-10T14:11:03.341010Z"
}
```

FlowMachine usage log:

```json
{
	"query_id": "ddc61a04f608dee16fff0655f91c2057",
	"query_kind": "daily_location",
	"message": "b'{\"request_id\":\"d2892489-8fb8-40ec-94e6-2467266a0226\",\"action\":\"get_query_kind\",\"query_id\":\"ddc61a04f608dee16fff0655f91c2057\"}'",
	"request_id": "d2892489-8fb8-40ec-94e6-2467266a0226",
	"params": { "query_id": "ddc61a04f608dee16fff0655f91c2057" },
	"event": "get_query_kind",
	"logger": "flowmachine_core.query_run_log",
	"level": "info",
	"timestamp": "2019-01-10T14:11:03.335437Z"
}

{
	"query_id": "ddc61a04f608dee16fff0655f91c2057",
	"retrieved_params": {
		"aggregation_unit": "admin3",
		"method": "last",
		"date": "2016-01-01",
		"level": "admin3",
		"subscriber_subset": "all"
	},
	"message": "b'{\"request_id\":\"d2892489-8fb8-40ec-94e6-2467266a0226\",\"action\":\"get_params\",\"query_id\":\"ddc61a04f608dee16fff0655f91c2057\"}'",
	"request_id": "d2892489-8fb8-40ec-94e6-2467266a0226",
	"params": { "query_id": "ddc61a04f608dee16fff0655f91c2057" },
	"event": "get_params",
	"logger": "flowmachine_core.query_run_log",
	"level": "info",
	"timestamp": "2019-01-10T14:11:03.339602Z"
}

{
	"query_id": "ddc61a04f608dee16fff0655f91c2057",
	"message": "b'{\"request_id\":\"d2892489-8fb8-40ec-94e6-2467266a0226\",\"action\":\"get_sql\",\"query_id\":\"ddc61a04f608dee16fff0655f91c2057\"}'",
	"request_id": "d2892489-8fb8-40ec-94e6-2467266a0226",
	"params": { "query_id": "ddc61a04f608dee16fff0655f91c2057" },
	"event": "get_sql",
	"logger": "flowmachine_core.query_run_log",
	"level": "info",
	"timestamp": "2019-01-10T14:11:03.358644Z"
}
```

Note that the `request_id` field is identical across the five log entries, which lets you match the request across the multiple services.

### Error and Debugging Logs

FlowMachine and FlowAPI write logs to stderr. By default, the logging level is `error`. For more verbose logging, set the `FLOWMACHINE_LOG_LEVEL` and/or `FLOWAPI_LOG_LEVEL` environment variables to `info` or `debug` when starting the docker container(s).

Log messages from FlowMachine will show the `logger` field of the log entry as `flowmachine.debug`, and the Python module that emitted the log entry in the `submodule` field (e.g. `{'logger':'flowmachine.debug', 'submodule':'flowmachine.core.query'}`. FlowAPI debugging messages set `logger` to `flowapi.debug`.

### Managing and Monitoring Logs

Because FlowKit employs structured logging, and all log messages are JSON objects, the access and usage logs are easy to use with tools like [Logstash](https://www.elastic.co/products/logstash).

[Filebeat](https://www.elastic.co/docker-kubernetes-container-monitoring) allows you to integrate the logs from stdout and stderr directly into your monitoring system.