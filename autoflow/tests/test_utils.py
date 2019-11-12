# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest
import datetime
import pendulum
import json
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

from autoflow.utils import *


def test_get_output_filename():
    """
    Test that get_output_filename returns the expected filename when a tag is provided.
    """
    now = pendulum.parse("2016-01-01")
    with patch("pendulum.now", lambda x: now):
        output_filename = get_output_filename("dummy_filename.suffix", "DUMMY_TAG")
    assert output_filename == "dummy_filename__DUMMY_TAG__20160101T000000Z.suffix"


def test_get_output_filename_without_tag():
    """
    Test that get_output_filename returns the expected filename when a tag is not provided.
    """
    now = pendulum.parse("2016-01-01")
    with patch("pendulum.now", lambda x: now):
        output_filename = get_output_filename("dummy_filename.suffix")
    assert output_filename == "dummy_filename__20160101T000000Z.suffix"


def test_get_different_params_hash_for_different_parameters():
    """
    Test that get_params_hash gives different results for different parameters.
    """
    hash1 = get_params_hash({"DUMMY_PARAM_1": 1})
    hash2 = get_params_hash({"DUMMY_PARAM_2": 2})
    assert hash1 != hash2


def test_params_hash_independent_of_order():
    """
    Test that get_params_hash gives the same result if order of parameters changes.
    """
    hash1 = get_params_hash({"DUMMY_PARAM_1": 1, "DUMMY_PARAM_2": 2})
    hash2 = get_params_hash({"DUMMY_PARAM_2": 2, "DUMMY_PARAM_1": 1})
    assert hash1 == hash2


def test_get_params_hash_can_handle_dates():
    """
    Test that get_params_hash returns a result if the parameters contain dates,
    and that different dates produce different hashes.
    """
    hash1 = get_params_hash({"DUMMY_PARAM": pendulum.date(2016, 1, 1)})
    hash2 = get_params_hash({"DUMMY_PARAM": pendulum.date(2016, 1, 2)})
    assert hash1 != hash2


@pytest.mark.parametrize(
    "offset,reference_date,expected",
    [
        (
            datetime.date(2016, 1, 2),
            pendulum.date(2016, 1, 1),
            pendulum.date(2016, 1, 2),
        ),
        (-1, pendulum.date(2016, 1, 1), pendulum.date(2015, 12, 31)),
        (1, datetime.date(2016, 1, 1), pendulum.date(2016, 1, 2)),
    ],
)
def test_offset_to_date(offset, reference_date, expected):
    """
    Test that offset_to_date returns the expected Date object.
    """
    offset_date = offset_to_date(offset, reference_date)
    assert isinstance(offset_date, pendulum.Date)
    assert offset_date == expected


def test_offset_to_date_raises_type_error():
    """
    Test that offset_to_date raises a TypeError if offset has an invalid type.
    """
    with pytest.raises(
        TypeError, match="Invalid type for offset: expected 'date' or 'int', not 'str'"
    ):
        offset_date = offset_to_date("NOT_AN_OFFSET_OR_DATE", pendulum.date(2016, 1, 1))


def test_stencil_to_date_pairs():
    """
    Test that stencil_to_date_pairs returns expected date pairs.
    """
    stencil = [
        pendulum.date(2016, 1, 1),
        -4,
        [pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)],
        [-3, -1],
        [pendulum.date(2016, 1, 1), -1],
        [0, 0],
    ]
    reference_date = pendulum.date(2016, 1, 7)
    expected_date_pairs = [
        (pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 1)),
        (pendulum.date(2016, 1, 3), pendulum.date(2016, 1, 3)),
        (pendulum.date(2016, 1, 2), pendulum.date(2016, 1, 3)),
        (pendulum.date(2016, 1, 4), pendulum.date(2016, 1, 6)),
        (pendulum.date(2016, 1, 1), pendulum.date(2016, 1, 6)),
        (pendulum.date(2016, 1, 7), pendulum.date(2016, 1, 7)),
    ]
    date_pairs = stencil_to_date_pairs(stencil, reference_date)
    assert date_pairs == expected_date_pairs


@pytest.mark.parametrize(
    "stencil,error",
    [
        (["BAD_ELEMENT"], TypeError),
        ([[-1, "BAD_ELEMENT"]], TypeError),
        (-1, TypeError),
        ([[-3, -2, -1]], ValueError),
        ([[-1, -2]], InvalidDatePairError),
    ],
)
def test_stencil_to_date_pairs_errors(stencil, error):
    """
    Test that stencil_to_date_pairs raises the correct errors for invalid stencils.
    """
    reference_date = pendulum.date(2016, 1, 1)
    with pytest.raises(error):
        date_pairs = stencil_to_date_pairs(stencil, reference_date)


def test_stencil_to_set_of_dates():
    """
    Test that stencil_to_set_of_dates returns expected set of dates.
    """
    reference_date = pendulum.date(2016, 1, 7)
    stencil = [[pendulum.date(2016, 1, 1), -3], -1, 0]
    expected_set = set(pendulum.date(2016, 1, d) for d in [1, 2, 3, 4, 6, 7])
    set_of_dates = stencil_to_set_of_dates(stencil, reference_date)
    assert set_of_dates == expected_set


@pytest.mark.parametrize(
    "reference_date,available",
    [
        (pendulum.date(2016, 1, 5), False),
        (pendulum.date(2016, 1, 6), True),
        (pendulum.date(2016, 1, 7), False),
    ],
)
def test_dates_are_available(reference_date, available):
    stencil = [[pendulum.date(2016, 1, 2), -4], [-3, -1]]
    available_dates = [pendulum.date(2016, 1, d) for d in range(1, 6)]
    assert available == dates_are_available(stencil, reference_date, available_dates)


def test_get_session(monkeypatch):
    """
    Test that get_session returns a session that connects to the correct database.
    """
    db_name = "DUMMY_NAME"
    host = "DUMMY_HOST"
    password = "DUMMY_PASSWORD"
    port = 6666
    user = "DUMMY_USER"

    monkeypatch.setenv("AUTOFLOW_DB_PASSWORD", password)
    mock_psycopg2_connect = Mock()
    monkeypatch.setattr("psycopg2.connect", mock_psycopg2_connect)

    s = get_session(f"postgresql://{user}:{{}}@{host}:{port}/{db_name}")

    try:
        s.connection()
    except TypeError:
        # we get an exception because not a real
        # connection catching and ignoring
        pass

    mock_psycopg2_connect.assert_called_once_with(
        database=db_name, host=host, password=password, port=port, user=user
    )


def test_get_session_without_password(monkeypatch):
    """
    Test that get_session works for a passwordless database.
    """
    db_name = "DUMMY_NAME"
    host = "DUMMY_HOST"
    port = 6666
    user = "DUMMY_USER"

    mock_psycopg2_connect = Mock()
    monkeypatch.setattr("psycopg2.connect", mock_psycopg2_connect)

    s = get_session(f"postgresql://{user}@{host}:{port}/{db_name}")

    try:
        s.connection()
    except TypeError:
        # we get an exception because not a real
        # connection catching and ignoring
        pass

    mock_psycopg2_connect.assert_called_once_with(
        database=db_name, host=host, port=port, user=user
    )


def test_session_scope(monkeypatch):
    """
    Test that session_scope closes the session.
    """
    mock_session = Mock()
    monkeypatch.setattr("autoflow.utils.get_session", Mock(return_value=mock_session))
    with session_scope("sqlite:///") as session:
        pass
    assert session is mock_session
    mock_session.commit.assert_called_once()
    mock_session.close.assert_called_once()


def test_session_scope_raises(monkeypatch):
    """
    Test that session_scope rolls back the session if an exception is raised, and re-raises the exception.
    """
    mock_session = Mock()
    monkeypatch.setattr("autoflow.utils.get_session", Mock(return_value=mock_session))
    with pytest.raises(Exception, match="Dummy exception"), session_scope(
        "sqlite:///"
    ) as session:
        raise Exception("Dummy exception")
    mock_session.commit.assert_not_called()
    mock_session.rollback.assert_called_once()
    mock_session.close.assert_called_once()


@pytest.mark.parametrize(
    "before,expected",
    [
        (
            {"key": [1, 2.7, "string", True, None]},
            '{"key": [1, 2.7, "string", true, null]}',
        ),
        (pendulum.parse("2016-01-01", exact=True), '"2016-01-01"'),
        ((False, 1, "2", 3.0), '[false, 1, "2", 3.0]'),
    ],
)
def test_make_json_serialisable(before, expected):
    """
    Test that make_json_serialisable returns objects that can be serialised
    using the default json.dumps().
    """
    after = make_json_serialisable(before)
    assert json.dumps(after) == expected


def test_get_additional_parameter_names_for_notebooks():
    """
    Test that get_additional_parameter_names_for_notebooks returns the set of
    notebook parameters, excluding notebook labels.
    """
    notebooks = {
        "notebook1": {"parameters": {"p1": "DUMMY_PARAM_1"}},
        "notebook2": {
            "parameters": {
                "p1": "notebook1",
                "p2": "DUMMY_PARAM_1",
                "p3": "DUMMY_PARAM_2",
            }
        },
    }
    additional_params = get_additional_parameter_names_for_notebooks(notebooks)
    assert additional_params == {"DUMMY_PARAM_1", "DUMMY_PARAM_2"}


def test_get_additional_parameter_names_for_notebooks_with_reserved_parameter_names():
    """
    Test that get_additional_parameter_names_for_notebooks excludes reserved parameter names from the returned set.
    """
    notebooks = {
        "notebook": {"parameters": {"p1": "DUMMY_PARAM", "p2": "RESERVED_PARAM"}}
    }
    reserved_parameter_names = {"RESERVED_PARAM"}
    additional_params = get_additional_parameter_names_for_notebooks(
        notebooks, reserved_parameter_names
    )
    assert additional_params == {"DUMMY_PARAM"}


def test_get_additional_parameter_names_for_notebooks_raises_error():
    """
    Test that get_additional_parameter_names_for_notebooks raises an error if reserved parameter names are used as notebook labels.
    """
    notebooks = {"RESERVED_PARAM": {"parameters": {"p1": "DUMMY_PARAM"}}}
    reserved_parameter_names = {"RESERVED_PARAM"}
    with pytest.raises(ValueError):
        additional_params = get_additional_parameter_names_for_notebooks(
            notebooks, reserved_parameter_names
        )


def test_sort_notebook_labels():
    """
    Test that sort_notebook_labels sorts notebook labels into a correct order.
    """
    notebooks = {
        "notebook1": {"parameters": {"p1": "notebook2"}},
        "notebook2": {"parameters": {}},
        "notebook3": {"parameters": {"p1": "notebook1", "p2": "notebook2"}},
    }
    sorted_notebook_labels = sort_notebook_labels(notebooks)
    assert sorted_notebook_labels == ["notebook2", "notebook1", "notebook3"]


def test_sort_notebook_labels_circular_dependency():
    """
    Test that sort_notebooks_labels raises an error if notebooks have circular dependencies.
    """
    notebooks = {
        "notebook1": {"parameters": {"p1": "notebook2"}},
        "notebook2": {"parameters": {"p1": "notebook1"}},
    }
    with pytest.raises(
        ValueError, match="Notebook specifications contain cyclic dependencies."
    ):
        sorted_notebook_labels = sort_notebook_labels(notebooks)


def test_notebook_to_asciidoc(monkeypatch):
    """
    Test that notebook_to_asciidoc uses a nbconvert.ASCIIDocExporter with the
    specified template to convert the notebook.
    """
    open_mock = mock_open()
    nbformat_read_mock = Mock(return_value="DUMMY_NB")
    ExporterMock = Mock()
    ExporterMock.return_value.from_notebook_node.return_value = (
        "DUMMY_BODY",
        "DUMMY_RESOURCES",
    )
    monkeypatch.setattr("builtins.open", open_mock)
    monkeypatch.setattr("nbformat.read", nbformat_read_mock)
    monkeypatch.setattr("nbconvert.ASCIIDocExporter", ExporterMock)

    body, resources = notebook_to_asciidoc(
        notebook_path="DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb",
        asciidoc_template_path="DUMMY_TEMPLATE_PATH",
    )

    assert body == "DUMMY_BODY"
    assert resources == "DUMMY_RESOURCES"
    open_mock.assert_called_once_with("DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb")
    nbformat_read_mock.assert_called_once_with(open_mock(), as_version=4)
    assert open_mock().__exit__.called_once() or open_mock().close.called_once()
    ExporterMock.assert_called_once_with(template_file="DUMMY_TEMPLATE_PATH")
    ExporterMock.return_value.from_notebook_node.assert_called_once_with("DUMMY_NB")


def test_notebook_to_asciidoc_no_template(monkeypatch):
    """
    Test that notebook_to_asciidoc can be called without specifying a template
    file, and that the nbconvert default template will be used in this case.
    """
    ExporterMock = Mock()
    ExporterMock.return_value.from_notebook_node.return_value = (
        "DUMMY_BODY",
        "DUMMY_RESOURCES",
    )
    monkeypatch.setattr("builtins.open", mock_open())
    monkeypatch.setattr("nbformat.read", Mock())
    monkeypatch.setattr("nbconvert.ASCIIDocExporter", ExporterMock)

    body, resources = notebook_to_asciidoc(
        notebook_path="DUMMY_NOTEBOOKS_DIR/DUMMY_FILENAME.ipynb"
    )

    ExporterMock.assert_called_once_with()


def test_asciidoc_to_pdf(monkeypatch):
    """
    Test that asciidoc_to_pdf writes asciidoc file contents to temporary files,
    and runs asciidoctor_pdf on those files.
    """

    def asciidoctor_pdf_mock_side_effect(filename, o):
        filepath = Path(filename)
        assert filepath.exists()
        resource_path = filepath.parent / "DUMMY_RESOURCE_NAME"
        assert resource_path.exists()
        with open(filepath, "r") as f_body:
            assert "DUMMY_BODY" == f_body.read()
        with open(resource_path, "rb") as f_res:
            assert b"DUMMY_RESOURCE_CONTENT" == f_res.read()

    asciidoctor_pdf_mock = Mock(side_effect=asciidoctor_pdf_mock_side_effect)
    monkeypatch.setattr("autoflow.utils.asciidoctor_pdf", asciidoctor_pdf_mock)

    asciidoc_to_pdf(
        body="DUMMY_BODY",
        resources=dict(outputs={"DUMMY_RESOURCE_NAME": b"DUMMY_RESOURCE_CONTENT"}),
        output_path="DUMMY_OUTPUT_PATH",
    )

    asciidoctor_pdf_mock.assert_called_once()
    args, kwargs = asciidoctor_pdf_mock.call_args
    assert not Path(args[0]).exists()
    assert kwargs["o"] == "DUMMY_OUTPUT_PATH"
