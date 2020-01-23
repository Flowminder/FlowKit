## Caching in FlowKit

FlowKit implements a caching system to enhance performance. Queries requested via FlowAPI are cached in FlowDB, under the `cache` schema.

Once cached, a query will not be recalulated - the cached version will simply be returned instead, which can save significant computation time. In addition to queries which are _directly_ returned, FlowKit may cache queries which are used in calculating other queries. For example, calculating a modal location aggregate, and a daily location aggregate will both use the same underlying query when the dates (and other parameters) overlap. Hence, caching the underlying query allows both the aggregate and the modal location aggregate to be produced faster.

This performance boost is achieved at the cost of disk space usage. Flowmachine automatically manages the size of the on-disk cache, and will remove seldom used cache entries periodically. The frequency of this check can be configured using the `FLOWMACHINE_CACHE_PRUNING_FREQUENCY` environment variable. By default, this is set to `86400`, or 24 hours in seconds. For heavily used servers, it may be desirable to set this to a lower threshold. Automatic cache clearance follows the procedure described in the following section. 

When a query is requested via the API, the query itself will be cached along with all of the other queries on which its calculation depends. For complex queries this can result in a large number of tables being added to the cache. This default behaviour can be changed by setting the environment variable `FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING=true` when starting the FlowMachine server, which will result in only the specific queries requested being cached. Computation times may be significantly longer when dependency caching is turned off.

### Cache Management

FlowMachine and FlowDB provide tools to inspect and manage the content of FlowKit's cache. FlowDB also contains metadata about the content of cache, in the `cache.cached` table.

Administrators can inspect this table directly by connecting to FlowDB, but in many scenarios the better option is to make use of FlowMachine's [cache management module](../flowmachine/flowmachine/core/cache/).

The cache submodule provides functions to assess the disk usage of the cache tables, and to reduce the disk usage below a desired threshold.

#### Shrinking Cache

To identify which tables should be discarded from cache, FlowKit keeps track of how expensive they were to calculate initially, how much disk space they occupy, and how often and recently they have been used. These factors are combined into a cache score, based on the [cachey](https://github.com/dask/cachey) algorithm.

Each cache table has a cache score, with a higher score indicating that the table has more cache value.

FlowMachine provides two functions which make use of this cache score to reduce the size of the cache - [`shrink_below_size`](../flowmachine/flowmachine/core/cache/#shrink_below_size), and [`shrink_one`](../flowmachine/flowmachine/core/cache/#shrink_one). `shrink_one` flushes the table with the _lowest_ cache score. `shrink_below_size` flushes tables until the disk space used by the cache falls below a threshold[^1] by calling `shrink_one` repeatedly. By default, queries which have been recently calculated are *excluded* from removal. To configure the global default for the exclusion period, set the `CACHE_PROTECTED_PERIOD` environment variable for FlowDB, or update the `cache_protected_period` key in the `cache.cache_config` table. The default exclusion period is `86400`s (24 hours). This can also be overridden when calling the cache management functions directly.

If necessary, the cache can also be completely reset using the [`reset_cache`](../flowmachine/flowmachine/core/cache/#reset_cache) function.

#### Removing a Specific Query from Cache

If a specific query must be removed from the cache, then an administrator can use the [`invalidate_cache_by_id`](../flowmachine/flowmachine/core/cache/#invalidate_cache_by_id) function of the `cache` submodule.

By default, this function only removes that specific query from cache. However, setting the `cascade` argument to `True` will also flush from the cache any cached queries which used that query in their calculation. This will also cascade to any queries which used _those_ queries, and so on.

#### Configuring the Cache

There are three parameters which control FlowKit's cache, both of which are in the `cache.cache_config` table. `half_life` controls how much weight is given to recency of access when updating the cache score. `half_life` is in units of number of cache retrievals, so a larger value for `half_life` will give less weight to recency and frequency of access. 

!!! example
    `big_query` and `small_query` both took 100 seconds to calculate. `big_query` takes 100 bytes to store, and `small_query` takes 10 bytes.
    Their _costs_ are `compute_time/storage_size`, or 1 for `big_query` and 10 for `small_query`. `small_query` is stored first and has an initial cache score of 10.
    If query `big_query` is stored next, with a `half_life` of 2.0, it will get an initial cache score of 1.35. 
    
    Just in terms of the balance between compute time and storage cost, `small_query` is more valuable in cache because it is relatively cheaper to store.  However, after only four retrievals of `big_query` from cache, `big_query` will have a cache score of 13.3, meaning it is more valuable in cache because it is so frequently used.
    
    If `half_life` was instead set to 10.0, `big_query` would need to be retrieved _seven_ times to exceed the cache score of `small_query`. 
     
 
`cache_size` is the maximum size in bytes that the cache tables should occupy on disk. These settings default to 1000.0, and 10% of available space on the drive where `/var/lib/postgresql/data` is located.

`cache_protected_period` is the length of time in seconds that a cache table is, by default, immune from being removed by a cache shrinkage operation. This defaults to `86400`s, or 24 hours. During this time, cache tables will not be removed by automatic cache shrinking, and will be default be excluded from the cache management functions.

These values can be overridden when creating a new FlowDB container by setting the `CACHE_SIZE`, `CACHE_HALF_LIFE` and  `CACHE_PROTECTED_PERIOD` environment variables for the container, set by updating the `cache.cache_config` table after connecting directly to FlowDB, or modified using the cache submodule.

#### Redis and the Query Cache

FlowMachine also tracks the execution state of queries using redis. In some cases, it is possible for redis and the cache metadata table to get out of sync with one another (for example, if either redis or FlowDB has been manually edited). To deal with this, you can forcibly resync redis with FlowDB's cache table, using the `resync_redis_with_cache` function. This will reset redis, and repopulate it based _only_ on the contents of `cache.cached`.

!!! warning
    You _must_ ensure that no queries are currently running before using this function. Any queries that are currently running will become out of sync.    

[^1]:By default, this uses the value set for `cache_size` in `cache.cache_config`.