# Redaction strategy for labelled aggregation

Date: 08-12-2021

# Status

Proposed

## Context

At present, any rows in a spatial query are dropped if they return an aggregate of 15 subscribers or less. With new
labelling disaggregation queries being added to Flowkit, there is an increased risk of deanonymization attacks
using the increased resolution of information - we need to consider further redaction strategies to mitigate this. 

## Decision

For any labelled spatial aggregate queries, we drop any aggregation zone that contains any disaggregation less than 15
(for consistency with the rest of the dissagregation strategy).

## Consequences

This disaggregation strategy could reveal that a given zone contains less than 15 of _something_, by comparing a
disaggregated query to it's non-disaggregated counterpart.
This could lead to zones being dropped prematurely in disaggregations if a label choice leads to a small number of
subscribers appearing in a corner-case disaggregation
