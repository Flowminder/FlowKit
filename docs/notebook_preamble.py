# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This file is executed before running any Jupyter notebook during the docs build.
# It is set as the 'preamble' option to the 'mknotebooks' plugin in mkdocs.yml

import pandas as pd
import tabulate as tabulate
import pprint
import warnings
import os
import sys
from dotenv import find_dotenv
from datetime import timedelta

path_to_utils_module = os.path.join(
    os.path.dirname(find_dotenv()), "..", "integration_tests", "tests"
)
sys.path.insert(0, path_to_utils_module)

from utils import make_token

# Ignore warnings in notebook output

warnings.filterwarnings("ignore")


# Format pandas tables nicely


def to_md(self):
    return tabulate.tabulate(self.head(), self.columns, tablefmt="pipe")


def format_dict(x):
    return f'><div class="codehilite"><pre>{pprint.pformat(x)}</pre></div>'


get_ipython().display_formatter.formatters["text/html"].for_type(pd.DataFrame, to_md)
get_ipython().display_formatter.formatters["text/markdown"].for_type(dict, format_dict)
get_ipython().display_formatter.formatters["text/markdown"].for_type(
    str, lambda x: f">`{x}`"
)
get_ipython().display_formatter.formatters["text/markdown"].for_type(
    list, lambda x: f">`{x}`"
)

# Create an API access token

claims = {
    "daily_location": {
        "permissions": {"run": True, "poll": True, "get_result": True},
        "spatial_aggregation": ["admin3", "admin2"],
    },
    "modal_location": {
        "permissions": {"run": True, "poll": True, "get_result": True},
        "spatial_aggregation": ["admin3", "admin2"],
    },
    "flows": {
        "permissions": {"run": True, "poll": True, "get_result": True},
        "spatial_aggregation": ["admin3", "admin2", "admin1"],
    },
    "location_event_counts": {
        "permissions": {"run": True, "poll": True, "get_result": True},
        "spatial_aggregation": ["admin3", "admin2", "admin1"],
    },
    "meaningful_locations_aggregate": {
        "permissions": {"run": True, "poll": True, "get_result": True},
        "spatial_aggregation": ["admin3", "admin2", "admin1"],
    },
    "meaningful_locations_between_label_od_matrix": {
        "permissions": {"run": True, "poll": True, "get_result": True},
        "spatial_aggregation": ["admin3", "admin2", "admin1"],
    },
    "geography": {
        "permissions": {"run": True, "poll": True, "get_result": True},
        "spatial_aggregation": ["admin3", "admin2", "admin1"],
    },
    "total_network_objects": {
        "permissions": {"run": True, "poll": True, "get_result": True},
        "spatial_aggregation": ["admin3", "admin2", "admin1"],
    },
}

TOKEN = make_token(
    username="testuser", secret_key="secret", lifetime=timedelta(days=1), claims=claims
)
