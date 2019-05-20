# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
functions used for parsing global config
"""

import re
import yaml
from pathlib import Path

from etl.etl_utils import CDRType


def get_cdr_type_config(*, cdr_type: str, global_config: dict) -> dict:
    """
    Get the config for a particular type of CDR
    """

    return global_config["etl"][CDRType(cdr_type)]


def validate_config(*, global_config_dict: dict) -> Exception:

    keys = global_config_dict.keys()

    exceptions = []
    if "etl" not in keys:
        exceptions.append(ValueError("etl must be a toplevel key in the config file"))

    if "default_args" not in keys:
        exceptions.append(
            ValueError("default_args must be a toplevel key in the config file")
        )

    etl_keys = global_config_dict.get("etl", {}).keys()
    if etl_keys != CDRType._value2member_map_.keys():
        exceptions.append(
            ValueError(f"etl section must contain subsections for {list(CDRType)}")
        )

    for key, value in global_config_dict.get("etl", {}).items():
        if set(list(value.keys())) != set(["pattern", "concurrency"]):
            print(value)
            exceptions.append(
                ValueError(
                    f"Each etl subsection must contain a pattern and concurrency subsection - not present for {key}"
                )
            )

    if exceptions != []:
        raise ValueError(exceptions)


def get_config_from_file(*, config_filepath: Path):
    content = open(config_filepath, "r").read()
    return yaml.load(content, Loader=yaml.FullLoader)
