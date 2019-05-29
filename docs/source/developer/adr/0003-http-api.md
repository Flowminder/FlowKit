# HTTP API as Single Access Point

Date: 7 August 2018

# Status

Pending

## Context

Originally, the software tool that became FlowKit was designed to be used as an extensible library, which connected to one shared database. Users would extend their own copy of the library to add query types, or even modify existing ones.

This introduced considerable difficulties, e.g.: 
   - No guarantee that an analysis written by one person could be run by another, or by the same person in future.
   - All users required highly privileged access to the database
   - No way to manage usage of shared resource
   - No way to ensure that upstream changes were in use
   - Difficult to effectively exploit the ability to reuse already computed results between analysts
   - Significant complexity and blurred functional boundaries in the main library
   - Very difficult to use the tool outside the Python ecosystem
   - Substantial challenges in logging access and activity


This motivates the revised design, where there is a _single_ copy of the library responsible for constructing and running queries, accessed through a language neutral HTTP API. This facilitates some significant improvements:

   - Easy to produce clients for multiple language ecosystems
   - Can make substantial changes to the enclosed code, database structure etc. with very little disruption
   - Supports granular access control
   - Enables more secure storage of raw data, by removing direct access to the data
   - Allows for much more efficient sharing of resources
   - Supports comprehensive logging
   - Much clearer 'seams' between functional parts, and simpler codebase
   - Simpler code, because scheduling of query runs is controlled by a single point.
   - Considerable opportunities to be more efficient in scheduling runs and caching of queries.

## Decision

FlowMachine will be wrapped by an HTTP API.

## Consequences

Analysts have significantly reduced freedom to work with the database under this scheme, and the number of docker containers required to constitute a running FlowKit system is considerably increased.
