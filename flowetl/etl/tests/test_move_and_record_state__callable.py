from etl.production_task_callables import move_and_record_state__callable
from pathlib import Path


class FakeDagRun:
    def __init__(self, conf):
        self.conf = conf


def test_move_and_record_state__callable(tmpdir):

    from_dir = tmpdir.mkdir("from_dir")
    to_dir = tmpdir.mkdir("to_dir")

    file = from_dir.join(f"file_to_move")

    file_contents = """
    Some contents in
    a file..
    """
    file.write(file_contents)

    fake_dag_run = FakeDagRun(conf={"file_name": "file_to_move"})

    mount_paths = {"ingest": Path(from_dir), "archive": Path(to_dir)}

    move_and_record_state__callable(
        dag_run=fake_dag_run,
        mount_paths=mount_paths,
        from_dir="ingest",
        to_dir="archive",
    )

    assert len(from_dir.listdir()) == 0
    assert len(to_dir.listdir()) == 1

    moved_file = to_dir.listdir()[0]
    assert moved_file.read() == file_contents
