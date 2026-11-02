"""Transparent huge pages (THP) policy for a worker process.

With THP=always, each thread's mimalloc heap on a free-threaded build faults into 2 MB pages of the one big
anonymous arena that holds every Python object, which costs roughly 4x the memory. Linux only; elsewhere a no-op.
"""
import ctypes
import logging
import mmap
import os
import sys
import sysconfig

log = logging.getLogger("mayu.worker")

MODES = ("python", "keep", "off")

_MAPS = "/proc/self/maps"
_PR_SET_THP_DISABLE = 41
_MADV_NOHUGEPAGE = getattr(mmap, "MADV_NOHUGEPAGE", 15)  # the value is the same on every Linux architecture


def check(mode: str) -> str:
    if mode not in MODES:
        raise ValueError(f"bad thp mode {mode!r}: use one of {', '.join(MODES)}")
    return mode


def apply(mode: str) -> None:
    """Never raises for a system-call failure: a worker must still start."""
    check(mode)
    if mode == "keep" or not sys.platform.startswith("linux"):
        return
    if mode == "off":
        _prctl_disable()
    elif sysconfig.get_config_var("Py_GIL_DISABLED"):  # pymalloc, on GIL builds, is not affected
        _advise_python_arena()


def _libc():
    libc = ctypes.CDLL(None, use_errno=True)
    libc.madvise.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
    libc.prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]
    return libc


def _prctl_disable() -> None:
    try:
        libc = _libc()
        if libc.prctl(_PR_SET_THP_DISABLE, 1, 0, 0, 0) != 0:
            _warn("prctl(PR_SET_THP_DISABLE)")
    except (OSError, AttributeError) as e:
        log.warning("could not disable transparent huge pages: %s", e)


def _advise_python_arena() -> None:
    try:
        arena = _arena_of_python_objects()
        if arena is None:
            return
        start, end = arena
        if _libc().madvise(start, end - start, _MADV_NOHUGEPAGE) != 0:
            _warn("madvise(MADV_NOHUGEPAGE)")
    except (OSError, AttributeError, ValueError) as e:
        log.warning("could not opt Python's heap out of transparent huge pages: %s", e)


def _arena_of_python_objects() -> tuple[int, int] | None:
    """The anonymous writable mapping that holds a fresh object (mimalloc's arena), or None."""
    obj = object()
    addr = id(obj)
    with open(_MAPS) as f:
        for line in f:
            fields = line.split(maxsplit=5)
            lo, hi = (int(x, 16) for x in fields[0].split("-"))
            if lo <= addr < hi:
                # no path, inode 0; kernels with CONFIG_ANON_VMA_NAME show mimalloc's arena as "[anon:mimalloc]"
                unnamed = len(fields) == 5 or fields[5].startswith("[anon:")
                anonymous = unnamed and fields[4] == "0"
                return (lo, hi) if anonymous and "w" in fields[1] else None
    return None


def _warn(call: str) -> None:
    err = ctypes.get_errno()
    log.warning("%s failed: %s (transparent huge pages left as they were)", call, os.strerror(err))
