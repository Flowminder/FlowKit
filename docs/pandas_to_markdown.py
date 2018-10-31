# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd
import tabulate as tabulate


def to_md(self):
    return tabulate.tabulate(self.head(), self.columns, tablefmt="pipe")


get_ipython().display_formatter.formatters["text/html"].for_type(pd.DataFrame, to_md)
