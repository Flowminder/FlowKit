# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
functions used for parsing global config
"""
import yaml

from copy import deepcopy
from pathlib import Path

from etl.etl_utils import CDRType


def validate_config(global_config_dict: dict) -> None:
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

    for cdr_type, value in global_config_dict.get("etl", {}).items():
        if set(value.keys()) != set(["source", "concurrency"]):
            exc_msg = (
                "Each etl subsection must contain a 'source' and 'concurrency' "
                f"subsection - not present for '{cdr_type}'. "
                f"[DDD] value.keys(): {value.keys()}"
            )
            exceptions.append(ValueError(exc_msg))
        else:
            if "source_type" not in value["source"]:
                exceptions.append(
                    ValueError(
                        f"Subsection 'source' is is missing the 'source_type' key for cdr_type '{cdr_type}'."
                    )
                )
            else:
                if value["source"]["source_type"] not in ["csv", "sql"]:
                    exc_msg = f"Invalid source type: '{value['source']['source_type']}'. Allowed values: 'csv', 'sql'"
                    exceptions.append(ValueError(exc_msg))

                if value["source"]["source_type"] == "sql":
                    if "table_name" not in value["source"]:
                        exc_msg = f"Missing 'table_name' key in 'source' subsection of cdr type '{cdr_type}'."
                        exceptions.append(ValueError(exc_msg))

    if exceptions != []:
        raise ValueError(exceptions)


def fill_config_default_values(*, global_config_dict: dict) -> dict:
    global_config_dict = deepcopy(global_config_dict)

    for cdr_type, value in global_config_dict["etl"].items():
        if (
            value["source"]["source_type"] == "sql"
            and "sql_find_available_dates" not in value["source"]
        ):
            source_table = value["source"]["table_name"]
            default_sql = (
                f"SELECT DISTINCT event_time::date as date FROM {source_table}"
            )
            value["source"]["sql_find_available_dates"] = default_sql

    return global_config_dict


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
    content = config_filepath.open("r").read()
    return yaml.load(content, Loader=yaml.SafeLoader)
