from unittest.mock import patch, MagicMock
from pathlib import Path

from etl.production_task_callables import move_file_and_record_ingestion_state__callable


class FakeDagRun:
    def __init__(self, conf):
        self.conf = conf


def test_move_file_and_record_ingestion_state__callable(tmpdir):

    from_dir = tmpdir.mkdir("from_dir")
    to_dir = tmpdir.mkdir("to_dir")

    file_name = "file_to_move"
    file = from_dir.join(file_name)

    file_contents = """
    Some contents in
    a file..
    """
    file.write(file_contents)

    fake_dag_run = FakeDagRun(conf={"file_name": file_name})

    mount_paths = {"ingest": Path(from_dir), "archive": Path(to_dir)}
    mock_record_etl_state = MagicMock()

    assert len(mock_record_etl_state.mock_calls) == 0
    with patch("etl.production_task_callables.record_etl_state", mock_record_etl_state):
        move_file_and_record_ingestion_state__callable(
            dag_run=fake_dag_run,
            mount_paths=mount_paths,
            from_dir="ingest",
            to_dir="archive",
        )

    assert len(mock_record_etl_state.mock_calls) == 1
    _, _, kwargs = mock_record_etl_state.mock_calls[0]

    assert kwargs == {"file_name": file_name, "state": Path(to_dir).name}
    assert not from_dir.listdir()
    assert len(to_dir.listdir()) == 1

    moved_file = to_dir.listdir()[0]
    assert moved_file.read() == file_contents
