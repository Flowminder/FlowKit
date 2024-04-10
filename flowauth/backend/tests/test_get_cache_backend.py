# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from multiprocessing import Lock, Pipe, Process
from multiprocessing.connection import Connection

import pytest
from dogpile.cache.backends.file import DBMBackend
from dogpile.cache.backends.memory import MemoryBackend
from dogpile.cache.backends.redis import RedisBackend
from flowauth.config import get_cache_backend


def test_required_redis_args(monkeypatch):
    """
    Test that a redis host env var must be set with a redis backend.
    """
    monkeypatch.setenv("FLOWAUTH_CACHE_BACKEND", "redis")
    with pytest.raises(KeyError) as exc_info:
        get_cache_backend()
    assert exc_info.value.args[0] == "FLOWAUTH_REDIS_HOST"


def test_required_file_args(monkeypatch):
    """
    Test that a file path must be given when using a file backend.
    """
    monkeypatch.setenv("FLOWAUTH_CACHE_BACKEND", "file")
    with pytest.raises(KeyError) as exc_info:
        get_cache_backend()
    assert exc_info.value.args[0] == "FLOWAUTH_CACHE_FILE"


def test_redis_backend(monkeypatch):
    """
    Test that a redis backend is created.
    """
    monkeypatch.setenv("FLOWAUTH_CACHE_BACKEND", "redis")
    monkeypatch.setenv("FLOWAUTH_REDIS_HOST", "localhost")
    cache = get_cache_backend()
    assert isinstance(cache.actual_backend, RedisBackend)


def test_file_backend(monkeypatch, tmpdir):
    """
    Test that the file backend is created and works cross-process.
    """
    monkeypatch.setenv("FLOWAUTH_CACHE_BACKEND", "file")
    monkeypatch.setenv("FLOWAUTH_CACHE_FILE", tmpdir / "flowauth_cache")
    cache = get_cache_backend()
    assert isinstance(cache.actual_backend, DBMBackend)
    lock = Lock()
    lock.acquire()
    parent_conn, child_conn = Pipe()
    writer_proc = Process(target=write_key, args=(lock,))
    reader_proc = Process(target=read_key, args=(lock, child_conn))
    writer_proc.start()
    reader_proc.start()
    assert parent_conn.recv() == "TEST_VALUE"
    writer_proc.join()
    reader_proc.join()


def write_key(lock: Lock):
    cache = get_cache_backend()
    cache.set("TEST_KEY", "TEST_VALUE")
    lock.release()


def read_key(lock: Lock, connection: Connection):
    cache = get_cache_backend()
    with lock:
        connection.send(cache.get("TEST_KEY"))
