# Contributing to mayu

Thanks for helping. Bug reports, docs fixes, benchmarks and code are all welcome.

## Before you start

- **Bugs:** open an issue with the mayu version, Python build (`python -VV`, and whether it is
  free-threaded), OS, Redis version and a minimal reproduction.
- **Features and larger changes:** open an issue first so we can agree on the design before you
  write code. mayu keeps a small scope on purpose (see §7 of the
  [architecture spec](docs/specs/2026-10-11-mayu-architecture-design.md)).
- **Security issues:** don't open a public issue. See [SECURITY.md](SECURITY.md).

## Repository layout

| Path | What it is |
|---|---|
| `crates/mayu-core` | Rust runtime: dispatch, retry policy, rate limits, metrics. No PyO3 dependency. |
| `crates/mayu-redis` | Redis broker: streams, delayed set, dead letters, and the Lua scripts in `src/lua/`. |
| `crates/mayu-py` | PyO3 bindings, built as `mayu._native`. |
| `python/mayu` | The Python package: `App`, tasks, worker, supervisor, CLI. |
| `tests/python` | pytest suite (needs Redis). |
| `tests/chaos` | Crash and restart tests (needs Docker). |
| `bench` | Benchmarks against Celery, RQ, Dramatiq and streaQ. |

## Setup

You need Rust (1.85+), [uv](https://docs.astral.sh/uv/) and Docker (for a throwaway Redis).

```bash
scripts/redis-up.sh                                   # Redis on :6390 for tests
uv venv --python 3.14t .venv                          # free-threaded build
uv pip install --python .venv/bin/python maturin pytest msgpack opentelemetry-sdk pytest-run-parallel
VIRTUAL_ENV=$PWD/.venv .venv/bin/maturin develop --uv # rebuild after every Rust change
```

Tests read `MAYU_TEST_REDIS` (default `redis://127.0.0.1:6390/0`). `MAYU_TEST_BROKER=memory://` runs the Python suite on
in-process brokers instead (embedded mode), skipping tests that use Redis directly.

## Running tests

```bash
cargo test --workspace                                # Rust tests (need Redis)
.venv/bin/pytest                                      # Python tests
scripts/matrix.sh                                     # every CI leg: ft (3.14t), ft-stress, gil (3.14), py313 (3.13)
scripts/matrix.sh py313                               # one leg
scripts/test-all.sh                                   # Rust + matrix + reconnect + chaos
```

The `gil` test leg looks for a GIL interpreter at `.venv-gil` (or `MAYU_GIL_PYTHON`):

```bash
uv venv --python 3.14+gil .venv-gil && uv pip install --python .venv-gil/bin/python maturin pytest msgpack
VIRTUAL_ENV=$PWD/.venv-gil .venv-gil/bin/maturin develop --uv
```

Chaos tests: `.venv/bin/pytest tests/chaos -v` (`MAYU_CHAOS_N` and `MAYU_CHAOS_SECONDS` scale them).
Benchmarks: `bench/setup.sh && bench/all.sh`.

## Pull requests

1. Branch from `main` and keep the PR to one change.
2. Add or update tests. A bug fix starts with a test that fails without it.
3. Run `cargo test --workspace` and `scripts/matrix.sh` (at least the `ft` and `py313` legs).
4. Update `README.md` when behaviour or options change, and add a line to `CHANGELOG.md` under
   *Unreleased*.
5. Write commit messages as `area: what changed`, for example `fix(redis): ...`,
   `perf(core): ...`, `bench: ...`, `docs: ...`.

[AGENTS.md](AGENTS.md) lists the code-quality rules reviewers check. They apply to people too.

## Releasing (maintainers)

Bump `version` in `pyproject.toml` and `Cargo.toml`, move *Unreleased* in `CHANGELOG.md` under the
new version, then tag `vX.Y.Z` and push the tag. The release workflow builds wheels and the sdist
and publishes them to PyPI through trusted publishing.

## License

By contributing, you agree that your contributions are dual licensed under MIT OR Apache-2.0, as
described in the [README](README.md#license).
