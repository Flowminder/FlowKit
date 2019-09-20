# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
functions used for parsing global config
"""
import yaml

from pathlib import Path

from etl.etl_utils import CDRType


def validate_config(*, global_config_dict: dict) -> Exception:
    """
    Function used to validate the config.yml file. Makes sure we
    have entries for each CDR type in CDRType enum and that each
    entry has expected information. Either raises Exceptions or
    passes silently.

    Parameters
    ----------
    global_config_dict : dict
        dict containing global config for ETL
    """
    keys = global_config_dict.keys()

    exceptions = []
    if "etl" not in keys:
        exceptions.append(ValueError("etl must be a toplevel key in the config file"))

    if "default_args" not in keys:
        exceptions.append(
            ValueError("default_args must be a toplevel key in the config file")
        )

    etl_keys = global_config_dict.get("etl", {}).keys()
    if not set(etl_keys).issubset(CDRType):
        unexpected_keys = list(set(etl_keys).difference(CDRType))
        exceptions.append(
            ValueError(
                f"Etl sections present in config.yml must be a subset of {[x.value for x in CDRType]}. "
                f"Unexpected keys: {unexpected_keys}"
            )
        )

    for key, value in global_config_dict.get("etl", {}).items():
        if set(value.keys()) != set(["source", "concurrency"]):
            exc_msg = (
                "Each etl subsection must contain a 'source' and 'concurrency' "
                f"subsection - not present for '{key}'. "
                f"[DDD] value.keys(): {value.keys()}"
            )
            exceptions.append(ValueError(exc_msg))

    if exceptions != []:
        raise ValueError(exceptions)


def get_config_from_file(*, config_filepath: Path) -> dict:
    """
    Function used to load configuration from YAML file.

    Parameters
    ----------
    config_filepath : Path
        Location of the file config.yml

    Returns
    -------
    dict
        Yaml config loaded into a python dict
    """
    content = open(config_filepath, "r").read()
    return yaml.load(content, Loader=yaml.SafeLoader)
