# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd
import tabulate as tabulate
import pprint
import warnings

warnings.filterwarnings("ignore")


def to_md(self):
    return tabulate.tabulate(self.head(), self.columns, tablefmt="pipe")


def format_dict(x):
    return f'><div class="codehilite"><pre>{pprint.pformat(x)}</pre></div>'


get_ipython().display_formatter.formatters["text/html"].for_type(pd.DataFrame, to_md)
get_ipython().display_formatter.formatters["text/markdown"].for_type(dict, format_dict)
get_ipython().display_formatter.formatters["text/markdown"].for_type(
    str, lambda x: f">`{x}`"
)
