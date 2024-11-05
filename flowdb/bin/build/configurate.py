# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
configurate.py

Automatic sensible defaults for flowdb installation. Expects to find a template file
with placeholder fields:

- `cores` (sets to floor(0.9*n_cores))
- `workers` (ceil(cores/2)
- `preloads` (contains pg_stat_activity and plugin_debugger if the DEBUG env var is set)
- `effective_cache_size` (75% of total memory)
- `shared_buffers` (25% of total memory up to a max of 16GB)
- `gendate` (Run time stamp of this script)
- `stats_target` (default_statistics_target)
- `use_jit` (enable/disable jit)
"""

import datetime
from math import ceil, floor
import os
import psutil


def _humansize(nbytes):
    """
    Script to humanize byte inputs. This is useful
    for creating the type of input required in
    PostgreSQL's postgres.local.conf file.

    Original code comes from:

    * https://stackoverflow.com/questions/14996453/\
      python-libraries-to-calculate-human-readable-filesize-from-bytes

    """
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
    if nbytes == 0:
        return "0 B"
    i = 0
    while nbytes >= 1024 and i < len(suffixes) - 1:
        nbytes /= 1024.0
        i += 1
    f = ("%.2f" % ceil(nbytes)).rstrip("0").rstrip(".")
    return "%s%s" % (f, suffixes[i])


def bool_env(var):
    try:
        return os.getenv(var, False).lower() == "true"
    except AttributeError:
        return False


total_mem = psutil.virtual_memory().total
shared_buffers = os.getenv(
    "SHARED_BUFFERS_SIZE",
    _humansize(ceil(0.25 * total_mem)) if total_mem < 64000000000 else "16GB",
)
cores = int(os.getenv("MAX_CPUS", floor(0.9 * psutil.cpu_count())))
workers = int(os.getenv("MAX_WORKERS", ceil(cores / 2)))
workers_per_gather = int(os.getenv("MAX_WORKERS_PER_GATHER", ceil(cores / 2)))
effective_cache_size = os.getenv(
    "EFFECTIVE_CACHE_SIZE", _humansize(ceil(0.75 * total_mem))
)
use_jit = "off" if bool_env("NO_USE_JIT") else "on"
stats_target = int(
    os.getenv("STATS_TARGET", 10000)
)  # Default to higher than pg default
max_locks = int(os.getenv("MAX_LOCKS_PER_TRANSACTION", 365 * 5 * 4 * (1 + 4)))


config_path = f"/flowdb_autoconf/{os.getenv('AUTO_CONFIG_FILE_NAME', 'postgresql.configurator.conf')}"

preload_libraries = ["pg_stat_statements"]
if bool_env("FLOWDB_ENABLE_POSTGRES_DEBUG_MODE"):
    preload_libraries.append("plugin_debugger")

possible_log_destinations = ["stderr", "jsonlog", "csvlog"]
log_destination = os.getenv("FLOWDB_LOG_DEST", "jsonlog").lower()
if log_destination not in possible_log_destinations:
    raise ValueError(
        f"Invalid log destination. Valid values for FLOWDB_LOG_DEST are {possible_log_destinations}"
    )

with open("/docker-entrypoint-initdb.d/pg_config_template.conf") as fin:
    config_file = fin.read().format(
        cores=cores,
        workers=workers,
        workers_per_gather=workers_per_gather,
        preloads=",".join(preload_libraries),
        effective_cache_size=effective_cache_size,
        shared_buffers=shared_buffers,
        gendate=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        stats_target=stats_target,
        use_jit=use_jit,
        max_locks=max_locks,
        log_destination=log_destination,
        collecter_on="on" if log_destination != "stderr" else "off",
    )

print("Writing config file to", config_path)
print("Writing\n", config_file)
with open(config_path, "w") as fout:
    fout.write(config_file)
