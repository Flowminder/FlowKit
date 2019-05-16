# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for configuration parsing
"""

from unittest.mock import Mock, patch
from pathlib import Path

import yaml
import pytest

from etl.etl_utils import CDRType
from etl.config_constant import config
from etl.config_parser import get_cdr_type_config, validate_config


def test_get_cdr_type_config_with_valid_type():
    """
    Test that we can get the config for a given CDR type
    """
    cdr_type = "calls"
    cdr_type_config = get_cdr_type_config(cdr_type=cdr_type, global_config=config)

    assert cdr_type_config == config["etl"][cdr_type]


def test_get_cdr_type_config_with_invalid_type():
    """
    Test that if we try to get a non existent CDR type as defined
    by etl.etl_utils.CDRType we gat an exception.
    """
    cdr_type = "spaghetti"

    with pytest.raises(ValueError):
        cdr_type_config = get_cdr_type_config(cdr_type=cdr_type, global_config=config)


def test_config_validation():
    """
    Check that with valid config dict we get no exception
    """
    validate_config(global_config_dict=config)


def test_config_validation_fails_no_etl_section():
    """
    Check that we get an exception raised if etl subsection
    missing. The exception will also contain two other exceptions.
    One for missing etl section and one for missing etl subsections.
    """
    bad_config = config.copy()
    bad_config.pop("etl")

    with pytest.raises(ValueError) as raised_exception:
        validate_config(global_config_dict=bad_config)

    assert len(raised_exception.value.args[0]) == 2


def test_config_validation_fails_no_default_args_section():
    """
    Check that we get an exception raised if default args
    subsection missing.
    """
    bad_config = config.copy()
    bad_config.pop("default_args")

    with pytest.raises(ValueError) as raised_exception:
        validate_config(global_config_dict=bad_config)

    assert len(raised_exception.value.args[0]) == 1


def test_config_validation_fails_bad_etl_subsection():
    """
    Check that we get an exception raised if an etl subsection
    does not contain correct keys.
    """
    bad_config = config.copy()
    bad_config["etl"]["calls"].pop("pattern")

    with pytest.raises(ValueError) as raised_exception:
        validate_config(global_config_dict=bad_config)

    assert len(raised_exception.value.args[0]) == 1
