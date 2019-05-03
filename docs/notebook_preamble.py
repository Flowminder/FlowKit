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
import git
from datetime import timedelta

path_to_utils_module = os.path.join(
    git.Repo(".", search_parent_directories=True).working_tree_dir,
    "integration_tests",
    "tests",
)
sys.path.insert(0, path_to_utils_module)

from utils import make_token, all_access_claims

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

TOKEN = make_token(
    username="docsuser",
    secret_key="secret",
    lifetime=timedelta(days=1),
    claims=all_access_claims,
)
