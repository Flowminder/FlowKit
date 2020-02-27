# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This file is executed before running any Jupyter notebook during the docs build.
# It is set as the 'preamble' option to the 'mknotebooks' plugin in mkdocs.yml

import pandas as pd
import pprint
import warnings
import logging
import os
from flowkit_jwt_generator.jwt import (
    generate_token,
    get_all_claims_from_flowapi,
    load_private_key,
)
from datetime import timedelta


# Ignore warnings in notebook output

warnings.filterwarnings("ignore")


# Suppress flowmachine log messages
logging.getLogger().setLevel(logging.ERROR)


# Format pandas tables nicely


def to_md(df, max_colwidth=50):
    """
    Wrapper around `DataFrame.to_markdown`, to display MultiIndex headers on multiple lines,
    and truncate entries longer than max_colwidth characters.
    """
    if df.columns.nlevels == 1:
        col_headers = map(str, df.columns.tolist())
    else:
        # If multi-level column headers, split over multiple lines
        col_headers = ("<br>".join(map(str, col)) for col in df.columns.tolist())
    # Add column level names in left-hand column
    headers = ["<br>".join((name or "" for name in df.columns.names))] + list(
        col_headers
    )
    if any(df.index.names):
        # Add index name (if there is one) on a new header line
        headers[0] += "<br><br>" + ", ".join(map(str, df.index.names))
        headers[1:] = [h + "<br><br>&nbsp;" for h in headers[1:]]

    # Truncate entries wider than max_colwidth
    return (
        df.astype(str)
        .apply(
            lambda x: [
                (a if len(a) <= max_colwidth else a[: max_colwidth - 3] + "...")
                for a in x
            ]
        )
        .to_markdown(headers=headers)
    )


def format_dict(x):
    return f'><div class="codehilite"><pre>{pprint.pformat(x)}</pre></div>'


get_ipython().display_formatter.formatters["text/markdown"].for_type(
    pd.DataFrame, to_md
)
get_ipython().display_formatter.formatters["text/markdown"].for_type(dict, format_dict)
get_ipython().display_formatter.formatters["text/markdown"].for_type(
    str, lambda x: f">`{x}`"
)
get_ipython().display_formatter.formatters["text/markdown"].for_type(
    list, lambda x: f">`{x}`"
)

# Create an API access token


TOKEN = generate_token(
    username="docsuser",
    private_key=load_private_key(os.environ["PRIVATE_JWT_SIGNING_KEY"]),
    lifetime=timedelta(days=1),
    claims=get_all_claims_from_flowapi(flowapi_url="http://localhost:9090"),
    flowapi_identifier=os.environ["FLOWAPI_IDENTIFIER"],
)
