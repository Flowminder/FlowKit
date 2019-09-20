# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for configuration parsing
"""
import pendulum
import pytest
import textwrap
import yaml

from copy import deepcopy
from pathlib import Path

from etl.config_parser import validate_config, get_config_from_file
from etl.etl_utils import (
    find_files,
    extract_date_from_filename,
    find_files_matching_pattern,
)


def test_config_validation(sample_config_dict):
    """
    Check that with valid config dict we get no exception
    """
    validate_config(global_config_dict=sample_config_dict)


def test_config_validation_fails_no_etl_section(sample_config_dict):
    """
    Check that we get an exception raised if etl subsection
    missing. The exception will also contain two other exceptions.
    One for missing etl section and one for missing etl subsections.
    """
    bad_config = deepcopy(sample_config_dict)
    bad_config.pop("etl")

    with pytest.raises(ValueError) as raised_exception:
        validate_config(global_config_dict=bad_config)

    assert len(raised_exception.value.args[0]) == 1


def test_config_validation_fails_for_invalid_etl_section(sample_config_dict):
    bad_config = deepcopy(sample_config_dict)
    bad_config["etl"]["foobar"] = {}

    expected_error_msg = (
        "Etl sections present in config.yml must be a subset of \['calls', 'sms', 'mds', 'topups'\]. "
        "Unexpected keys: \['foobar'\]"
    )
    with pytest.raises(ValueError, match=expected_error_msg):
        validate_config(global_config_dict=bad_config)


def test_config_validation_fails_no_default_args_section(sample_config_dict):
    """
    Check that we get an exception raised if default args
    subsection missing.
    """
    bad_config = deepcopy(sample_config_dict)
    bad_config.pop("default_args")

    with pytest.raises(ValueError) as raised_exception:
        validate_config(global_config_dict=bad_config)

    assert len(raised_exception.value.args[0]) == 1


def test_config_validation_fails_bad_etl_subsection(sample_config_dict):
    """
    Check that we get an exception raised if an etl subsection
    does not contain correct keys.
    """
    bad_config = deepcopy(sample_config_dict)
    bad_config["etl"]["calls"].pop("source")

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

    tmpdir_path_obj = Path(tmpdir)

    files = find_files(files_path=tmpdir_path_obj)

    assert set([file.name for file in files]) == set(["A.txt", "B.txt"])


def test_find_files_non_default_filter(tmpdir):
    """
    Test that find files returns correct files
    with non-default filter argument.
    """
    tmpdir.join("A.txt").write("content")
    tmpdir.join("B.txt").write("content")
    tmpdir.join("README.md").write("content")

    tmpdir_path_obj = Path(tmpdir)

    files = find_files(files_path=tmpdir_path_obj, ignore_filenames=["B.txt", "A.txt"])

    assert set([file.name for file in files]) == set(["README.md"])


def test_find_files_matching_pattern(tmpdir):
    """
    Test that find_files_matching_pattern() returns correct files.
    """
    tmpdir.join("A_01.txt").write("content")
    tmpdir.join("A_02.txt").write("content")
    tmpdir.join("B_01.txt").write("content")
    tmpdir.join("B_02.txt").write("content")
    tmpdir.join("README.md").write("content")

    tmpdir_path_obj = Path(tmpdir)

    files = find_files_matching_pattern(
        files_path=tmpdir_path_obj, filename_pattern="(.)_01.txt"
    )
    assert ["A_01.txt", "B_01.txt"] == files

    files = find_files_matching_pattern(
        files_path=tmpdir_path_obj, filename_pattern="A_.*\.txt"
    )
    assert ["A_01.txt", "A_02.txt"] == files

    files = find_files_matching_pattern(
        files_path=tmpdir_path_obj, filename_pattern=".*"
    )
    assert ["A_01.txt", "A_02.txt", "B_01.txt", "B_02.txt", "README.md"] == files

    files = find_files_matching_pattern(
        files_path=tmpdir_path_obj, filename_pattern="foobar.txt"
    )
    assert [] == files


def test_get_config_from_file(tmpdir):
    """
    Test that we can load yaml to dict from file
    """
    sample_dict = {"A": 23, "B": [1, 2, 34], "C": {"A": "bob"}}
    config_dir = tmpdir.mkdir("config")
    config_file = config_dir.join("config.yml")
    config_file.write(yaml.dump(sample_dict))

    config = get_config_from_file(config_filepath=Path(config_file))
    assert config == sample_dict


def test_extract_date_from_filename():
    filename = "CALLS_20160101.csv.gz"
    filename_pattern = r"CALLS_(\d{8}).csv.gz"
    date_expected = pendulum.Date(2016, 1, 1)
    date = extract_date_from_filename(filename, filename_pattern)
    assert date_expected == date

    filename = "SMS__2018-04-22.csv.gz"
    filename_pattern = r"SMS__(\d{4}-[0123]\d-\d{2}).csv.gz"
    date_expected = pendulum.Date(2018, 4, 22)
    date = extract_date_from_filename(filename, filename_pattern)
    assert date_expected == date

    filename = "foobar.csv.gz"
    filename_pattern = r"SMS_(\d{8}).csv.gz"
    with pytest.raises(
        ValueError, match="Filename 'foobar.csv.gz' does not match the pattern"
    ):
        extract_date_from_filename(filename, filename_pattern)
