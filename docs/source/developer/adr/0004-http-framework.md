# Quart as HTTP Framework

Date: 7 August 2018

# Status

Pending

## Context

In order to wrap the FlowKit toolkit in a single HTTP API, an HTTP framework is required. There are a variety of options for this purpose, e.g. Flask and Django.

Both Flask & Django offer a significant plugin ecosystem, and are 'battle-tested'. However, both are on the heavy side and are bound by legacy design. Of the two, Flask has much less boilerplate overhead.

An alternative option is Quart, which is considerably newer. Quart is compatible with the Flask ecosystem plugin, and built to follow the newer ASGI standard. It is lightweight, offers impressive performance, and takes full advantage of the recent addition of asyncio to Python.

Quart also supports websockets, which, while not an immediate priority, are likely to be very useful for future, more dynamic iterations of the API.

Should Quart become defunct, the close mapping to the Flask API provides a low-impact exit. 

## Decision

The FlowKit API will make use of the Quart framework.

## Consequences

The API implementation is somewhat tied to asyncio.
