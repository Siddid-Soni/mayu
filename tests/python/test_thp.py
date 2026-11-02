import ctypes
import logging
import subprocess
import sys
import sysconfig

import pytest

from mayu import thp
from mayu.worker import WorkerRunner

LINUX = sys.platform.startswith("linux")
FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

# monkeypatch swaps module attributes that every thread of a pytest-run-parallel run shares
thread_unsafe = pytest.mark.thread_unsafe(reason="patches mayu.thp for the whole process")


class FakeLibc:
    def __init__(self, rc=0, errno=0):
        self.calls = []
        self.rc, self.errno = rc, errno

    def madvise(self, *args):
        return self._call("madvise", args)

    def prctl(self, *args):
        return self._call("prctl", args)

    def _call(self, name, args):
        self.calls.append((name, *args))
        ctypes.set_errno(self.errno)
        return self.rc


def _vmflags_of_mapping_holding(obj) -> list[str]:
    addr, flags, on = id(obj), [], False
    with open("/proc/self/smaps") as f:
        for line in f:
            head = line.split()[0]
            if "-" in head and not head.endswith(":"):
                lo, hi = (int(x, 16) for x in head.split("-"))
                on = lo <= addr < hi
            elif on and head == "VmFlags:":
                flags = line.split()[1:]
    return flags


@pytest.mark.skipif(not (LINUX and FREE_THREADED), reason="only free-threaded Linux builds are advised")
def test_python_mode_opts_the_object_arena_out_of_huge_pages():
    obj = object()
    thp.apply("python")
    assert "nh" in _vmflags_of_mapping_holding(obj)
    thp.apply("python")  # idempotent
    assert "nh" in _vmflags_of_mapping_holding(obj)


@thread_unsafe
@pytest.mark.skipif(not (LINUX and FREE_THREADED), reason="only free-threaded Linux builds are advised")
def test_python_mode_advises_exactly_the_anonymous_mapping_holding_python_objects(monkeypatch):
    libc = FakeLibc()
    monkeypatch.setattr(thp, "_libc", lambda: libc)
    anchor = object()
    thp.apply("python")
    (name, start, length, advice), = libc.calls
    assert name == "madvise" and advice == thp._MADV_NOHUGEPAGE
    assert start <= id(anchor) < start + length


@thread_unsafe
@pytest.mark.parametrize(
    "tail, found",
    [
        ("", True),
        ("   [anon:mimalloc]", True),  # kernels with CONFIG_ANON_VMA_NAME (Linux 7.0 on ubuntu-26.04)
        ("   [anon:some name]", True),
        ("   [heap]", False),
        ("   /usr/lib/libfoo.so", False),
    ],
)
def test_arena_detection_accepts_named_anonymous_mappings_only(monkeypatch, tmp_path, tail, found):
    maps = tmp_path / "maps"
    maps.write_text(f"0-7fffffffffffffff rw-p 00000000 00:00 0{tail}\n")
    monkeypatch.setattr(thp, "_MAPS", str(maps))
    assert thp._arena_of_python_objects() == ((0, 0x7FFFFFFFFFFFFFFF) if found else None)


@thread_unsafe
@pytest.mark.skipif(not LINUX or FREE_THREADED, reason="GIL builds use pymalloc, which is unaffected")
def test_python_mode_does_nothing_on_gil_builds(monkeypatch):
    libc = FakeLibc()
    monkeypatch.setattr(thp, "_libc", lambda: libc)
    thp.apply("python")
    assert libc.calls == []


@thread_unsafe
@pytest.mark.parametrize("platform", ["linux", "darwin"])
def test_keep_makes_no_system_call(monkeypatch, platform):
    libc = FakeLibc()
    monkeypatch.setattr(thp, "_libc", lambda: libc)
    monkeypatch.setattr(sys, "platform", platform)
    thp.apply("keep")
    assert libc.calls == []


@thread_unsafe
def test_off_disables_thp_for_the_whole_process(monkeypatch):
    libc = FakeLibc()
    monkeypatch.setattr(thp, "_libc", lambda: libc)
    monkeypatch.setattr(sys, "platform", "linux")
    thp.apply("off")
    assert libc.calls == [("prctl", 41, 1, 0, 0, 0)]


@thread_unsafe
@pytest.mark.parametrize("mode", ["python", "off"])
def test_nothing_happens_off_linux(monkeypatch, mode):
    libc = FakeLibc()
    monkeypatch.setattr(thp, "_libc", lambda: libc)
    monkeypatch.setattr(sys, "platform", "darwin")
    thp.apply(mode)
    assert libc.calls == []


@pytest.mark.skipif(not LINUX, reason="THP is a Linux feature")
def test_off_shows_in_proc_status_of_a_real_process():
    code = (
        "from mayu import thp\n"
        "thp.apply('off')\n"
        "import subprocess, sys\n"
        "def flag(text): return [l for l in text.splitlines() if l.startswith('THP_enabled')]\n"
        "print(flag(open('/proc/self/status').read()))\n"
        "child = subprocess.run(['cat', '/proc/self/status'], capture_output=True, text=True).stdout\n"
        "print(flag(child))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    if "THP_enabled" not in out.stdout:  # kernels before 5.0 do not report it
        pytest.skip("kernel does not report THP_enabled")
    assert out.stdout.splitlines() == ["['THP_enabled:\\t0']", "['THP_enabled:\\t0']"]


@pytest.mark.parametrize("mode", ["", "on", "PYTHON", "none", None])
def test_unknown_mode_is_a_clear_value_error(mode):
    with pytest.raises(ValueError, match=r"bad thp mode .*python, keep, off"):
        thp.apply(mode)


def test_worker_runner_validates_thp_in_the_constructor():
    from types import SimpleNamespace

    app = SimpleNamespace(result_ttl=None, queues=lambda **kw: ["q"], registry={}, pools={})
    with pytest.raises(ValueError, match="bad thp mode 'bad'"):
        WorkerRunner(app, threads=1, thp="bad")
    assert WorkerRunner(app, threads=1).thp == "python"
    assert WorkerRunner(app, threads=1, thp="off").thp == "off"


@thread_unsafe
@pytest.mark.skipif(not LINUX, reason="THP is a Linux feature")
@pytest.mark.parametrize("mode", ["python", "off"])
def test_a_failing_system_call_logs_a_warning_and_does_not_raise(monkeypatch, caplog, mode):
    if mode == "python" and not FREE_THREADED:
        pytest.skip("python mode only acts on free-threaded builds")
    monkeypatch.setattr(thp, "_libc", lambda: FakeLibc(rc=-1, errno=22))
    with caplog.at_level(logging.WARNING, logger="mayu.worker"):
        thp.apply(mode)
    assert any(r.levelno == logging.WARNING and "Invalid argument" in r.getMessage() for r in caplog.records)


@thread_unsafe
@pytest.mark.skipif(not LINUX, reason="THP is a Linux feature")
def test_a_missing_libc_symbol_logs_a_warning_and_does_not_raise(monkeypatch, caplog):
    def broken():
        raise OSError("no libc")

    monkeypatch.setattr(thp, "_libc", broken)
    with caplog.at_level(logging.WARNING, logger="mayu.worker"):
        thp.apply("off")
    assert any("no libc" in r.getMessage() for r in caplog.records)
