# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# Create an API access token

import os
import sys
from dotenv import find_dotenv

path_to_utils_module = os.path.join(
    os.path.dirname(find_dotenv()), "..", "integration_tests", "tests"
)
sys.path.insert(0, path_to_utils_module)

from datetime import timedelta
from utils import make_token

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
}

TOKEN = make_token(
    username="testuser", secret_key="secret", lifetime=timedelta(days=1), claims=claims
)
