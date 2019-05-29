# Interprocess Communication (IPC) Methods - zeromq and redis

Date: 7 August 2018

# Status

Pending

## Context

With the revised design of FlowKit to follow an API-backend model, inter-service, and inter-process communication (IPC) must be considered. Previously, this was only relevant within a single multithreaded instance, for which python's built in `RLock` sufficed. 

`RLock` implements a re-entrant lock. A lock, in this context is something which may be 'held' by one task, to control access to some set of resources - while one task is holding the lock, any other task must wait for it to be released in order to access the same resources. In a standard lock, this applies _within_ a thread, so for example this -

```python
from threading import Lock
lock = Lock()
with lock:
    print("Got the lock once.")
    with lock:
        print("Got the lock twice.")
```

Will get stuck after printing "Got the lock once.", because the lock is not released. This restriction is relaxed with an `RLock`, because it can be held _multiple_ times by the same _thread_. So if the `Lock` instance above were replaced with `RLock`, the code would print twice and finish.

`RLock` is used in `flowmachine` to ensure that while a query is being written to cache other queries using it will wait until the cached version is available, instead of running it again. This has some limitations - `RLock` functions only with threads, and within a single instance of a program. 

In the API-backend model, a method of communication between the API server and the backend is required - this indicates a message queue type model, because the services will run in separate docker containers and communicate over the network. This is also desirable because it frees us from assuming a single backend server - while this is currently the typical case, it is pragmatic to not design out alternatives. Several message queues are available, with fairly standard capabilities. Of these zeromq is the lightest weight, lowest complexity option.

Considering IPC on the backend (flowmachine) is necessary because backend communication wth the database is multithreaded, and triggered by asynchronous requests from the API but the database is transactional. The backend needs to know whether a particular query is in the process of being calculated, but this cannot be reliably determined directly from the database itself, hence a method of indicating what is currently running is required. One might argue for idempotency (i.e. just run the query again) as the solution to this, but the considerable runtime of many queries suggests otherwise.


 
For IPC on the backend, a similar rationale to the use of a message queue applies - thread based primitives suffice, but close off the avenue of multiple backend servers. This indicates a key-value type approach, which can be used to the same end. Again, multiple options exist in the space, which are largely equivalent. Of these, redis is arguably the best known.   

## Decision

API-backend communication will be via zeromq, IPC in the backend will be mediated by redis.

## Consequences

Necessary to deal with securing inter-container communication via zeromq, and to manage an additional docker container for redis.
