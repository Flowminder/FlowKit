# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for configuration parsing
"""
import yaml
import pytest

from uuid import uuid1
from unittest.mock import Mock, patch
from pathlib import Path
from pendulum import parse
from copy import deepcopy

from etl.etl_utils import (
    CDRType,
    find_files,
    generate_temp_table_names,
    parse_file_name,
)
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
    bad_config = deepcopy(config)
    bad_config.pop("etl")

    with pytest.raises(ValueError) as raised_exception:
        validate_config(global_config_dict=bad_config)

    assert len(raised_exception.value.args[0]) == 2


def test_config_validation_fails_no_default_args_section():
    """
    Check that we get an exception raised if default args
    subsection missing.
    """
    bad_config = deepcopy(config)
    bad_config.pop("default_args")

    with pytest.raises(ValueError) as raised_exception:
        validate_config(global_config_dict=bad_config)

    assert len(raised_exception.value.args[0]) == 1


def test_config_validation_fails_bad_etl_subsection():
    """
    Check that we get an exception raised if an etl subsection
    does not contain correct keys.
    """
    bad_config = deepcopy(config)
    bad_config["etl"]["calls"].pop("pattern")

    with pytest.raises(ValueError) as raised_exception:
        validate_config(global_config_dict=bad_config)

    assert len(raised_exception.value.args[0]) == 1


def test_find_files_default_filter(tmpdir):
    """
    Test that find files returns correct files
    with default filter argument.
    """
    tmpdir.join("A.txt").write("content")
    tmpdir.join("B.txt").write("content")
    tmpdir.join("README.md").write("content")

    tmpdir_path_obj = Path(dump_path=tmpdir)

    files = find_files(tmpdir_path_obj)

    assert set([file.name for file in files]) == set(["A.txt", "B.txt"])


def test_find_files_default_filter(tmpdir):
    """
    Test that find files returns correct files
    with non-default filter argument.
    """
    tmpdir.join("A.txt").write("content")
    tmpdir.join("B.txt").write("content")
    tmpdir.join("README.md").write("content")

    tmpdir_path_obj = Path(tmpdir)

    files = find_files(dump_path=tmpdir_path_obj, ignore_filenames=["B.txt", "A.txt"])

    assert set([file.name for file in files]) == set(["README.md"])


@pytest.mark.parametrize(
    "cdr_type", [CDRType("calls"), CDRType("sms"), CDRType("mds"), CDRType("topups")]
)
def test_generate_temp_table_names(cdr_type):
    """
    Test that we are able to generate correct temp table names for each cdr_type
    """
    uuid = uuid1()

    table_names = generate_temp_table_names(uuid=uuid, cdr_type=cdr_type)

    uuid_sans_underscore = str(uuid).replace("-", "")
    assert table_names == {
        "extract_table": f"etl.x{uuid_sans_underscore}",
        "transform_table": f"etl.t{uuid_sans_underscore}",
        "load_table": f"events.{cdr_type}",
    }


@pytest.mark.parametrize(
    "file_name,want",
    [
        (
            "CALLS_20160101.csv.gz",
            {"cdr_type": CDRType("calls"), "cdr_date": parse("20160101")},
        ),
        (
            "SMS_20160101.csv.gz",
            {"cdr_type": CDRType("sms"), "cdr_date": parse("20160101")},
        ),
        (
            "MDS_20160101.csv.gz",
            {"cdr_type": CDRType("mds"), "cdr_date": parse("20160101")},
        ),
        (
            "TOPUPS_20160101.csv.gz",
            {"cdr_type": CDRType("topups"), "cdr_date": parse("20160101")},
        ),
    ],
)
def test_parse_file_name(file_name, want):

    cdr_type_config = config["etl"]
    got = parse_file_name(file_name=file_name, cdr_type_config=cdr_type_config)
    assert got == want
