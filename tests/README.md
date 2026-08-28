# Tests

```bash
python tests/run.py              # everything, no dependencies beyond boto3
python tests/run.py logger gzip  # only modules matching these names
pytest tests/                    # same tests, nicer failure output, if you have pytest
```

The suite runs against `src/` in the working tree, not against an installed
copy, so it works anywhere the logger works — on the boat, over a slow link,
with nothing installed but boto3.

Nothing here touches a network or the real bucket. Every S3 interaction goes
through the fakes in `helpers.py`.

## What each module covers

| module | covers |
|---|---|
| `test_logger.py` | capture, buffer accounting and eviction, spool/upload, durability, supervision, and the systemd unit |
| `test_gzip.py` | RFC 1952 conformance, byte-reproducibility, content addressing, key layout |
| `test_readback.py` | `iter_rows_ndjson_gz` — correctness and memory shape on multi-GB day objects |
| `test_audit_log.py` | which tools write `_log/` entries, and which deliberately do not |
| `test_formats.py` | row shapes, field types, key and filename layouts, object headers, and agreement with `SCHEMA.md` |
| `test_pg.py` | the wide table: bucket grid, `last()` and its tie-break, the range guard, and DDL that grows a column |

## Why these tests exist

Most of them are regressions. Each pins down something that was once wrong,
and the docstrings say which — that is the point of them, and the reason to
keep the explanation next to the assertion rather than in a commit message
nobody will read again. The ones worth knowing about:

- **`test_frames_arriving_during_a_flush_are_never_lost`** — the worst bug
  found. Buffer eviction runs while a write is in flight, so removing "the N
  oldest by position" afterwards discarded newly arrived frames that had been
  written nowhere. Reproduced against the old code at 15 of 15 frames lost.
- **`test_a_dead_loop_exits_instead_of_hanging`** — `asyncio.gather` does not
  cancel its siblings, so a crashed flush loop left a live process that kept
  capturing and never uploaded again, with `Restart=always` never firing
  because nothing had exited.
- **`test_flush_fsyncs_both_the_file_and_the_directory`** — the atomic-write
  idiom is four steps and the code did three. Without the directory fsync the
  rename is not durable, and a power cut inside the journal commit window
  lost a batch the flush had already reported as safe.
- **`test_identical_content_produces_identical_bytes`** — gzip stamps a
  compression timestamp by default, so identical rows produced different
  bytes and a different ETag every time.
- **`test_peak_memory_...`** (`test_readback.py`) — reading back a day object
  once materialised the whole day in RAM.
- **`test_the_env_file_is_readable_by_systemd`** — `export KEY=value` is
  valid shell and invalid to systemd, which rejects the whole line and starts
  the service with none of its environment. The logger came up with no
  credentials at all and nothing in the code could tell.

## Conventions

Tests are plain functions with plain `assert`s and no framework-specific
constructs, which is what lets `run.py` and `pytest` both work.

They are **behavioural**: they drive the real functions and assert on what
was produced. A few assert on source text or on the unit file, all marked,
because the property being checked is the absence of something — a
subprocess in the capture path, an audit write in a read-only tool — which
has no runtime signal to observe.

Anything slow enough to notice prints its duration. The suite runs in a few
seconds; if a test starts taking longer than that, it is probably doing real
I/O by accident.
