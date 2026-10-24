import datetime as dt
import time

import pytest

from mayu import TaskFailed
from mayu.task import _expires_ms


def test_expires_ms_seconds_from_now():
    before = int(time.time() * 1000)
    got = _expires_ms(30)
    assert before + 30_000 <= got <= int(time.time() * 1000) + 30_000


def test_expires_ms_none_stays_none():
    assert _expires_ms(None) is None


def test_expires_ms_absolute_datetime():
    when = dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc)
    assert _expires_ms(when) == int(when.timestamp() * 1000)


def test_expires_ms_rejects_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        _expires_ms(dt.datetime(2030, 1, 1))


def test_expires_ms_clamps_datetime_before_epoch():
    old = dt.datetime(1969, 1, 1, tzinfo=dt.timezone.utc)
    assert _expires_ms(old) == 0  # already past, not a negative deadline


@pytest.mark.parametrize("bad", [object(), "soon", True, dt.timedelta(seconds=5)])
def test_expires_ms_rejects_non_numbers(bad):
    with pytest.raises(TypeError):
        _expires_ms(bad)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_expires_ms_rejects_non_finite(bad):
    with pytest.raises(ValueError, match="finite"):
        _expires_ms(bad)


def test_expires_reserved_as_task_parameter(app):
    with pytest.raises(TypeError, match="reserved"):
        @app.task
        def f(expires):
            return expires


def test_expired_task_is_never_run(app, running):
    ran = []

    @app.task
    def work():
        ran.append(1)
        return "done"

    past = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=10)
    h = work.send(expires=past)
    with running(app, threads=4):
        with pytest.raises(TaskFailed, match="task expired"):
            h.result(timeout=10)
    assert ran == []


def test_not_yet_expired_task_runs(app, running):
    @app.task
    def work():
        return "done"

    h = work.send(expires=60)
    with running(app, threads=4):
        assert h.result(timeout=10) == "done"
