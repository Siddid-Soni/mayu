import contextlib
import os
import uuid

import pytest

from mayu import App, gpu
from mayu.worker import WorkerRunner

# A worker in `--gpu-monitor auto` would pick up this machine's real GPU, and whatever else uses it would feed the
# VRAM guard and make tests flaky. test_gpu.py installs fake backends (or builds `gpu.Nvml` directly) instead.
gpu._create_nvml = lambda: None

REDIS_URL = os.environ.get("MAYU_TEST_REDIS", "redis://127.0.0.1:6390/0")
# MAYU_TEST_BROKER=memory:// gives every `app` its own in-process broker; tests that reach Redis themselves are skipped.
EMBEDDED = (os.environ.get("MAYU_TEST_BROKER") or "").lower() == "memory://"


def pytest_collection_modifyitems(config, items):
    if not EMBEDDED:
        return
    skip = pytest.mark.skip(reason="uses Redis directly (MAYU_TEST_BROKER=memory://)")
    for item in items:
        if "redis_url" in item.fixturenames:
            item.add_marker(skip)


@pytest.fixture
def redis_url():
    return REDIS_URL


@pytest.fixture
def queue():
    return f"t-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def app(queue):
    broker = f"memory://{queue}" if EMBEDDED else REDIS_URL
    return App(broker=broker, result_ttl=60, default_queue=queue)


@contextlib.contextmanager
def _running(app, threads=4, **kwargs):
    w = WorkerRunner(app, threads=threads, queues=kwargs.pop("queues", [app.default_queue]), **kwargs)
    w.start()
    try:
        yield w
    finally:
        w.stop()
        w.join()


@pytest.fixture
def running():
    return _running
