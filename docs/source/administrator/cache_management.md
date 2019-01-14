# Caching in FlowKit

FlowKit implements a caching system to enhance performance. Queries requested via FlowAPI are cached in FlowDB, under the `cache` schema.

Once cached, a query will not be recalulated - the cached version will simply be returned instead, which can save significant computation time. In addition to queries which are _directly_ returned, FlowKit may cache queries which are used in calculating other queries. For example, calculating a modal location aggregate, and a daily location aggregate will both use the same underlying query when the dates (and other parameters) overlap. Hence, caching the underlying query allows both the aggregate and the modal location aggregate to be produced faster.


This performance boost is achieved at the cost of disk space usage, and management of the cached data will sometimes be required.

## Cache Management

FlowMachine and FlowDB provide tools to inspect and manage the content of FlowKit's cache. FlowDB also contains metadata about the content of cache, in the `cache.cached` table.

Administrators can inspect this table directly by connecting to FlowDB, but in many scenarios the better option is to make use of FlowMachine's [cache management module](../../developer/api/flowmachine/core/cache/).

The cache submodule provides functions to assess the disk usage of the cache tables, and to reduce the disk usage below a desired threshold.

### Shrinking Cache

To identify which tables should be discarded from cache, FlowKit keeps track of how expensive they were to calculate initially, how much disk space they occupy, and how often and recently they have been used. These factors are combined into a cache score, based on the [cachey](https://github.com/dask/cachey) algorithm.

Each cache table has a cache score, with a higher score indicating that the table has more cache value.

FlowMachine provides two functions which make use of this cache score to reduce the size of the cache - [`shrink_below_size`](../../developer/api/flowmachine/core/cache/#shrink_below_size), and [`shrink_one`](../../developer/api/flowmachine/core/cache/#shrink_one). `shrink_one` flushes the table with the _lowest_ cache score. `shrink_below_size` flushes tables until the disk space used by the cache falls below a threshold[^1] by calling `shrink_one` repeatedly.

### Removing a Specific Query from Cache

If a specific query must be removed from the cache, then an administrator can retrieve the query's _object_ from FlowDB by using the [`get_query_by_id`](../../developer/api/flowmachine/core/cache/#get_query_by_id) function of the `cache` submodule, and calling the object's [`invalidate_db_cache`](../../developer/api/flowmachine/core/query/#invalidate_db_cache) method.

By default, calling this method will also flush from the cache any cached queries which used that query in their calculation. This will also cascade to any queries which used _those_ queries, and so on. Setting the `cascade` argument to `False` will remove only the one query.

### Configuring the Cache

There are two parameters which control FlowKit's cache, both of which are in the `cache.cache_config` table. `half_life` controls how much weight is given to recency of access when updating the cache score. `cache_size` is the maximum size in bytes that the cache tables should occupy on disk. These settings default to 1000, and 10% of available space on the drive where `/var/lib/postgresql/data` is located.

These values can be overridden when creating a new FlowDB container by setting the `CACHE_SIZE` and `CACHE_HALF_LIFE` environment variables for the container, set by updating the `cache.cache_config` table after connecting directly to FlowDB, or modified using the cache submodule.

    

[^1]:By default, this uses the value set for `cache_size` in `cache.cache_config`.