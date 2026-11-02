"""Embedded mode: memory:// brokers run producers and workers in one process, with no Redis.

Each test builds its own app on a random memory://<name>, so under ft-stress (4 threads per test) every thread
has its own broker, app and worker."""
import os
import queue as queue_mod
import subprocess
import sys
import threading
import time
import uuid

import pytest

import mayu
from mayu import App, Retry, TaskFailed


def _app(name: str | None = None, **kwargs) -> App:
    return App(broker=f"memory://{name or uuid.uuid4().hex}", result_ttl=60, **kwargs)


def _until(predicate, what: str, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    while True:
        got = predicate()
        if got:
            return got
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.01)  # bounded poll: what is awaited is published by a heartbeat, with no event to wait on


def test_send_then_result():
    app = _app()

    @app.task
    def add(a, b):
        return a + b

    with app.worker(threads=2):
        assert add.send(2, 3).result(timeout=10) == 5
        assert app.send(add.name, 4, 5).result(timeout=10) == 9


def test_retries_then_success():
    app = _app()
    calls = []

    @app.task(max_retries=5)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise Retry(countdown=0)
        return "ok"

    with app.worker(threads=2):
        assert flaky.send().result(timeout=10) == "ok"
    assert len(calls) == 3


def test_dead_letter_after_max_retries():
    app = _app()

    @app.task(max_retries=1)
    def boom():
        raise Retry(countdown=0, exc=RuntimeError("still broken"))

    with app.worker(threads=2):
        h = boom.send()
        with pytest.raises(TaskFailed):
            h.result(timeout=10)
    dead = app.client.dead_letters(app.default_queue)
    assert [d["id"] for d in dead] == [h.id]
    assert app.client.queue_stats(app.default_queue)["dead"] == 1


def test_dead_letters_replay_runs_the_task_again():
    app = _app()
    fail = threading.Event()
    fail.set()

    @app.task(max_retries=0)
    def sometimes():
        if fail.is_set():
            raise RuntimeError("down")
        return "up"

    with app.worker(threads=2):
        h = sometimes.send()
        with pytest.raises(TaskFailed):
            h.result(timeout=10)
        fail.clear()
        entry = app.client.dead_letters(app.default_queue)[0]["entry_id"]
        assert app.client.replay_dead(app.default_queue, [entry]) == 1
        assert h.result(timeout=10) == "up"


def test_links_run_after_success():
    app = _app()
    got = queue_mod.Queue()

    @app.task
    def double(x):
        return x * 2

    @app.task
    def record(value, tag):
        got.put((tag, value))

    with app.worker(threads=2):
        assert double.send(4, link=record.s("sum")).result(timeout=10) == 8
        assert got.get(timeout=10) == ("sum", 8)


def test_two_node_workflow():
    app = _app()

    @app.task
    def fetch():
        return "data"

    @app.task
    def store():
        return "stored"

    wf = app.workflow("pipeline")
    a = wf.add(fetch)
    b = wf.add(store, after=a)
    with app.worker(threads=2):
        h = wf.send()
        assert h.wait(timeout=10) == "done"
        assert h.node(a).result(timeout=10) == "data"
        assert h.node(b).result(timeout=10) == "stored"
        assert [w["id"] for w in app.client.workflows()] == [h.id]


def test_countdown_delays_the_task():
    app = _app()

    @app.task
    def stamp():
        return time.time()

    with app.worker(threads=2):
        sent = time.time()
        ran = stamp.send(countdown=0.5).result(timeout=10)
    assert ran - sent >= 0.45


def test_an_expired_task_is_never_run():
    app = _app()
    ran = []

    @app.task
    def work():
        ran.append(1)

    h = work.send(countdown=0.3, expires=0.05)  # expires while it waits for its countdown
    with app.worker(threads=2):
        with pytest.raises(TaskFailed, match="task expired"):
            h.result(timeout=10)
    assert ran == []


def test_rate_limit_spaces_out_starts():
    app = _app()
    starts = []

    @app.task(rate_limit="5/s")
    def tick():
        starts.append(time.monotonic())

    with app.worker(threads=4):
        for h in [tick.send() for _ in range(4)]:
            h.result(timeout=10)
    starts.sort()
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert min(gaps) >= 0.15, gaps


def test_resources_admit_one_task_at_a_time():
    app = _app()
    spans = []
    lock = threading.Lock()

    @app.task(resources={"slot": 1})
    def hold():
        start = time.monotonic()
        time.sleep(0.1)  # the task's work: long enough that an overlap would show
        with lock:
            spans.append((start, time.monotonic()))

    with app.worker(threads=4, resources={"slot": 1}):
        for h in [hold.send(), hold.send()]:
            h.result(timeout=10)
    (s1, e1), (s2, e2) = sorted(spans)
    assert s2 >= e1, spans


def test_progress_is_visible_through_the_handle():
    app = _app()
    go = threading.Event()
    reported = threading.Event()

    @app.task
    def steps():
        mayu.progress(1, 4)
        reported.set()
        go.wait(10)
        return "ok"

    with app.worker(threads=2, heartbeat_interval=0.05) as w:
        h = steps.send()
        assert reported.wait(10)
        p = _until(lambda: (q := h.progress()) is not None and q.done == 1 and q, "the progress report")
        assert (p.done, p.total, p.worker) == (1, 4, w.worker_id)
        go.set()
        assert h.result(timeout=10) == "ok"
        _until(lambda: h.progress() is None, "the finished task to leave the published list")


def test_lazy_pool_loads_on_demand():
    app = _app()
    built = []

    @app.pool("model", vram_gb=4)
    def model(ctx):
        built.append(ctx.index)
        return f"model-{ctx.index}"

    @app.task
    def infer(x, m=app.use("model")):
        return f"{m}:{x}"

    with app.worker(threads=2, resources={"vram_gb": 4}, pools={"model": 1}, pool_mode="lazy"):
        assert built == [], "lazy: nothing is built until a task needs it"
        assert infer.send(1).result(timeout=10) == "model-0:1"
        assert infer.send(2).result(timeout=10) == "model-0:2"
    assert built == [0]


def test_apps_with_the_same_name_share_a_broker():
    name = uuid.uuid4().hex
    producer, worker_app = _app(name), _app(name)

    @worker_app.task(name="shared.echo")
    def echo(x):
        return x

    with worker_app.worker(threads=2):
        assert producer.send("shared.echo", "hi").result(timeout=10) == "hi"


def test_apps_with_different_names_do_not_share():
    producer, other = _app(), _app()

    @other.task(name="shared.echo")
    def echo(x):
        return x

    h = producer.send("shared.echo", "hi")
    with other.worker(threads=2):
        with pytest.raises(TimeoutError):
            h.result(timeout=0.5)
    assert producer.client.queue_stats(producer.default_queue)["stream"] == 1


def test_process_mode_is_refused():
    app = _app()

    @app.task
    def noop():
        pass

    with pytest.raises(ValueError, match="mode='thread'"):
        app.worker(mode="process")
    with pytest.raises(ValueError, match="mode must be"):
        app.worker(mode="fork")


@pytest.mark.parametrize("url", ["memory://a/b", "memory://a?x=1", "memory:///a", "memory://h:1"])
def test_anything_after_the_name_is_rejected(url):
    with pytest.raises(ValueError, match="takes only a name"):
        App(broker=url)


@pytest.mark.parametrize("command", [["worker"], ["inspect"], ["dead", "list"], ["dead", "replay"]])
def test_cli_refuses_a_memory_broker(command, tmp_path):
    (tmp_path / "embedded_app.py").write_text("from mayu import App\napp = App(broker='memory://cli')\n")
    env = {**os.environ, "PYTHONPATH": str(tmp_path), "MAYU_GPU_MONITOR": "off"}
    env.pop("MAYU_BROKER", None)
    out = subprocess.run([sys.executable, "-m", "mayu", *command, "embedded_app:app"], env=env, cwd=tmp_path,
                         capture_output=True, text=True, timeout=60)
    assert out.returncode != 0
    assert "inside one Python process" in out.stderr
    if command == ["worker"]:
        assert "app.worker()" in out.stderr
