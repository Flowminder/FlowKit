# JWTs for API Access Control

Date: 7 August 2018

# Status

Pending

## Context

Authentication and access control for FlowKit has previously been very limited, making use of only of postgres usernames and passwords. WIth the introduction of the API, we can be much more granular in allowing access through the use of [JSON Web Tokens](https://jwt.io).

These are an encoded and cryptographically signed string, which permit access to some set of API functionality. The string specifies the identity of the user, exactly what they can access, and the time period for which the token is valid.

Tampering with the token will result in the signature not matching, an event which can be logged an subsequently investigated.

A key advantage of JWTs is that they can be centrally managed, even where the service they will be used with is not accessible to the internet. Another significant advantage is that the tokens are inherently ephemeral - should a token be stolen, the time window for it to lead to a data breach will (given appropriate usage of the system) be small.

JWTs are also advantageous in that the add relatively minimal overhead to the day to day experience of analysts, and have broad support across many languages.

But perhaps the most important advantage is the granularity of access they afford - for example it is possible to allow access to only one specific result with this method. This is useful where developing dashboards and the like, or where access to outputs but not data must be provided to other parties.

Other alternatives would be to use a more traditional login system, backed by a database, integrate with the Docker host's authentication system, or to use public key based authentication. All of these require more and local administration, or necessitate the use of either a JWT equivalent to actually provide API access, or the use of cookies to avoid a need to log in for every communication with the API. 

## Decision

API authentication and access control will use JWTs.

## Consequences

Need to bear in mind upper size limits of JWTs when designing their structure, produce and maintain an additional service for managing generation of JWTs, and manage secure distribution of secret keys.
 