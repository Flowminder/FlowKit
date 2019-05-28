# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Tests for configuration parsing
"""
import pytest
import yaml

from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid1
from pendulum import parse

from etl.model import ETLRecord
from etl.config_constant import config
from etl.config_parser import validate_config, get_config_from_file
from etl.etl_utils import (
    CDRType,
    find_files,
    generate_table_names,
    parse_file_name,
    filter_files,
)


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
def test_generate_table_names(cdr_type):
    """
    Test that we are able to generate correct temp table names for each cdr_type
    """
    uuid = uuid1()

    table_names = generate_table_names(uuid=uuid, cdr_type=cdr_type)

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
    """
    Test we can parse cdr_type and cdr_date
    from filenames based on cdr type config.
    """
    cdr_type_config = config["etl"]
    got = parse_file_name(file_name=file_name, cdr_type_config=cdr_type_config)
    assert got == want


def test_parse_file_name_exception():
    """
    Test that we get a value error if filename does
    not match any pattern
    """
    cdr_type_config = config["etl"]
    file_name = "bob.csv"
    with pytest.raises(ValueError):
        parse_file_name(file_name=file_name, cdr_type_config=cdr_type_config)


def test_filter_files(session, monkeypatch):
    """
    testing the filter files function

    - set calls on 20160101 to quarantine so should not be filtered
    - set calls on 20160102 to archive so should be filtered
    - no record for sms 20160101 so should not be filtered
    - bad_file.bad doesn't match any pattern so should  be filtered
    """
    cdr_type_config = config["etl"]

    file1 = Path("./CALLS_20160101.csv.gz")
    file2 = Path("./CALLS_20160102.csv.gz")
    file3 = Path("./SMS_20160101.csv.gz")
    file4 = Path("./bad_file.bad")

    found_files = [file1, file2, file3, file4]

    # add a some etl records
    file1_data = {
        "cdr_type": "calls",
        "cdr_date": parse("2016-01-01").date(),
        "state": "quarantine",
    }

    file2_data = {
        "cdr_type": "calls",
        "cdr_date": parse("2016-01-02").date(),
        "state": "archive",
    }

    ETLRecord.set_state(
        cdr_type=file1_data["cdr_type"],
        cdr_date=file1_data["cdr_date"],
        state=file1_data["state"],
        session=session,
    )

    ETLRecord.set_state(
        cdr_type=file2_data["cdr_type"],
        cdr_date=file2_data["cdr_date"],
        state=file2_data["state"],
        session=session,
    )

    monkeypatch.setattr("etl.etl_utils.get_session", lambda: session)
    filtered_files = filter_files(
        found_files=found_files, cdr_type_config=cdr_type_config
    )
    assert filtered_files == [file1, file3]


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
