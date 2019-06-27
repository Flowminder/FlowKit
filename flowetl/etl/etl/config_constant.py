# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Canonical specification of sample config
"""

calls_config = {"pattern": r"CALLS_(\d{4}[01]\d[0123]\d).csv.gz", "concurrency": 4}
sms_config = {"pattern": r"SMS_(\d{4}[01]\d[0123]\d).csv.gz", "concurrency": 4}
mds_config = {"pattern": r"MDS_(\d{4}[01]\d[0123]\d).csv.gz", "concurrency": 4}
topups_config = {"pattern": r"TOPUPS_(\d{4}[01]\d[0123]\d).csv.gz", "concurrency": 4}

config = {
    "default_args": {"owner": "flowminder", "start_date": "1900-01-01"},
    "etl": {
        "calls": calls_config,
        "sms": sms_config,
        "mds": mds_config,
        "topups": topups_config,
    },
}
