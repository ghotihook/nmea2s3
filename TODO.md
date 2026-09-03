# TODO

Known issues and deferred decisions, newest review 2026-09-03. Nothing here
is a guess: every entry names the file and line it lives at, and says how it
was established — read, measured, or reproduced. Every line reference below
was re-checked against the tree on 2026-09-03; they drift with every commit,
so treat a mismatch as this file being stale rather than the issue being
fixed.

**Priority is ordered by what the failure costs, not by effort.** This
project has one asymmetry that decides everything: a frame the logger misses
does not exist anywhere, while everything downstream re-runs from the
archive. So the tiers are

| tier | means |
|---|---|
| **P0** | wrong data reaches storage, or capture loses frames. Not recoverable by re-running |
| **P1** | correct data, wrong answers — a derived table or a document that misleads someone acting on it |
| **P2** | works, but a foreseeable condition makes it fail confusingly |
| **P3** | deferred decisions, tidiness, coverage |

---

## P0 — wrong data, or lost frames

### 3. A partial write can duplicate rows into the archive

`src/nmea2s3/logger.py:729-731` — `_objects()` (`:340`) splits by capture day, so a
flush spanning UTC midnight yields two objects. If `_write_spool` places
object 1 and fails on object 2 it returns `False`; `_upload_objects` then
re-sends both, and on a part-way failure `_return_buffer(batch)` returns the
*whole* batch to RAM including frames already durable. The next flush regroups
them with newer arrivals, producing a different content id and so a different
key.

The only place the "identical rows always resolve to the same key, so a retry
is a harmless overwrite" invariant does not hold. `_write_spool` (`:860`)
guards it (`if path.exists(): continue`, `:877`); `_upload_objects` (`:785`)
and `_return_buffer` (`:618`) have no equivalent. Needs a midnight-spanning flush *and* a partial failure,
so it is narrow — but either track which objects were placed and return only
the rest, or document that this path can duplicate.

---

## P1 — correct data, misleading answers

### 4. `sql/metrics.sql` assumes the table is in `public`

`sql/metrics.sql:161` `GRANT USAGE ON SCHEMA public` and an unqualified
`FROM observations` (`:137`), while
`nmea2s3-update-pg` writes to wherever `search_path` resolves — which nothing
in the repo pins. The last remaining schema assumption after `353fc0e` fixed
the same class of bug in `table.ensure()`.

### 6. `--table` cannot be schema-qualified

`table.py:33` `SAFE_NAME` rejects a `.`, so `--table analytics.observations`
fails with a message about decoder field ids. One line in the flag's help.

---

## P2 — foreseeable conditions that fail confusingly

### 7. A malformed key aborts the whole export

`src/nmea2s3/ndjson.py:248,255` — `key_date` and `key_proto` raise
`ValueError` on any object under `raw/` that does not match the layout, from
inside the `iter_keys` generator. That is consumed by the `for key in keys:`
statement in `export_source`, *outside* the per-object `try`, so one stray
object defeats the skip-and-continue design around it.

`iter_log_keys` had the identical bug and was fixed in `9a1e3ef` — a legacy
`_log/` key aborted the whole audit-log export — so the two readers now
disagree about whether an unparseable key is fatal. The fix there is the
template: skip the key, do not end the listing.

### 8. `sql/` is unreachable after the documented install

`README.md:265` says `psql "$PG" -f sql/metrics.sql`, but the Configure
section correctly notes "pipx installs the commands, not the repo" and gives
`curl` lines for `env.example` and the unit file only. `sql/` is also missing
from `[tool.hatch.build.targets.sdist].include` (`pyproject.toml:69`), which
does list `tests` and `systemd`. Add a third `curl` line and `sql` to the
include.

### 9. Two names for the connection string, neither defined

`README.md:265` uses `$PG`; `sql/metrics.sql:19` uses `$DATABASE_URL`.
`env.example` defines neither — it exposes five discrete `NMEA2S3_PG_*` vars,
which `psql` does not read. Pick one and show how to build it.

### 10. Export's atomic write drops the fourth step

`src/nmea2s3/export.py:293` — file fsync then rename, no directory fsync,
while the comment above it claims "same tmp-then-rename reasoning as the disk
spool", whose own `_fsync_dir()` in `logger.py` calls that step "the easiest
one to leave out". Low stakes — an export re-runs — but the comment
claims a parity the code does not have.

### 11. `n2k_spare` reaches a column

`src/nmea2s3/pg/wire_n2k.py:184` filters `"reserved" in f.id.lower()`, but
SPARE is a separate field type and is not caught. One word in the condition.
Observed on PGN 60928.

### 12. ISO Address Claim fills columns with bus-management metadata

60928 contributes `n2k_uniquenumber`, `n2k_deviceinstancelower`,
`n2k_deviceinstanceupper`, `n2k_systeminstance` and friends — plain NUMBERs,
so they predate the lookup work; `ac7cb5c` added three `_code` columns to the
same PGN. If address-claim traffic clutters the table the fix is a PGN-level
skip, not a field-level one. Deliberately not done: no evidence yet that it
is a problem in practice.

### 19. The importer needs a read-capable key, which its design does not

`src/nmea2s3/migrate_n0183.py` reconciles each day with `object_exists()`,
and HEAD is `s3:GetObject`. Under a put+list key a MISSING object answers 404
(fine) and a PRESENT one answers 403 `AccessDenied`, which is in
`NON_RETRYABLE_CODES` — so it ends the run, and does it precisely in the
second-run case the whole no-state-file design exists for. Documented in the
module docstring for now (`98938e7`, then `9a1e3ef`), because the operator
running an import has a read-capable key anyway.

Reconciling with a `list_objects_v2` on the day prefix instead would drop the
read requirement entirely and match what the module says about its own
credentials. Worth doing only if this ever has to run under the logger's
own put-only key.

---

## P3 — deferred decisions and coverage

### 13. Test coverage gaps

- **`src/nmea2s3/retry.py` has no tests at all.** It owns
  `NON_RETRYABLE_CODES`, the "raise the last exception, never silently give
  up" contract, and the jitter — all pure functions of a fake callable, and
  the read path has no higher-level fallback behind it.
- **`src/nmea2s3/pg/update.py` has no tests.** The ledger *is* the
  exactly-once promise (`--rebuild`, `record_key`, and the
  `key_date`-then-filename sort that fast-packet reassembly depends on). The
  `FakeCon` in `test_pg.py` would cover it.
- **`export.py`'s CSV and ndjson writers**, the `-o` tmp→rename path, the
  skip-and-continue handlers and the exit-2 path. Only `candump` and the
  argparse guards are tested — and `candump` is the newest, least
  load-bearing format.

`migrate_n0183.py` is no longer on this list: `tests/test_migrate_n0183.py`
covers the record, the determinism the re-run model depends on, the dry run,
and the start/end audit pair.

### 14. `ensure()` is called twice per object

`pg/update.py:221` and again inside `table.write()` (`table.py:115`), so every
object costs two `CREATE TABLE IF NOT EXISTS` + catalogue round trips. Having
`write()` return the columns it added would drop half. Only reason for the
outer call is capturing `added` for the run summary.

### 15. BITLOOKUP and INDIRECT_LOOKUP are still dropped

`src/nmea2s3/pg/wire_n2k.py:34` — deliberate. A bitfield wants a mask column,
not a code; an indirect lookup means nothing without the field it depends on.
Both need a naming rule of their own, so they wait for a use.

### 16. Wind-angle convention unverified against real traffic

`src/nmea2s3/pg/ranges.py:26-36` — which convention the `nmea2000` library
returns for PGN 130306 has not been confirmed. The wire format is 0..2π,
which would make those fields bearings rather than signed angles. Check the
first real day where the n2k and 0183 streams overlap.

The comment there no longer contradicts the bound — `98938e7` made it say
that `(-180, 360)` is deliberately wide because this is unverified, and a
bound is the wrong place to find out, since it drops every reading instead of
reporting one. Once the first real day settles it, the bound can tighten to
`(-180, 180]` and that note comes out.

### 17. A batch larger than the whole spool budget is written anyway

`src/nmea2s3/logger.py:887` — no truncation implemented; eviction clears
every other spool file trying to make room and usage still exceeds
`MAX_DISK_BYTES`. Only reachable if `MAX_BUFFER_BYTES` is ever configured
close to or above `MAX_DISK_BYTES`, which it is not (48 MB against 2 GB).
Recorded so the constraint between the two constants is not forgotten.

### 18. `systemd/nmea2s3.service` hardcodes `can0` twice

`:37` in `ExecStartPre` and `:39` in `ExecStart --can can0`. Changing the
interface silently requires editing both, and nothing in the header says so.
The spool avoided the same trap by moving to `--disk-dir`, which the unit
passes once; the interface still cannot, because `ExecStartPre` runs before
the process exists and has to name it independently.

(The second half of this item — `README.md` calling the unit a template with
"three placeholders" — went away with the root/global rewrite: there are no
placeholders left to miscount.)

### 20. The key-layout rule is stated twice, never once

`raw/` puts its discriminator in the object NAME; `_log/` puts it in the
PATH. Both are right for their access pattern — "everything on this day" is
what a reader asks of `raw/`, "everything this tool did" is what it asks of
`_log/`, and each layout puts its dominant filter first — but `SCHEMA.md`
argues the two independently, so the pair reads as an inconsistency rather
than one rule applied twice. State the rule once (the thing you filter on
most goes earliest in the key) so the next prefix has a principle to apply
instead of two precedents to choose between. Layout itself reviewed and kept
2026-09-03; this is about the writing.

---

## Done — 2026-09-03

| commit | what |
|---|---|
| `7b0aff5` | The unit ran as an unprivileged user from `~/.local/bin`. Now root from a `pipx --global` install, spooling to `/var/lib/nmea2s3` — the default `Path.home()/n2k_fallback` is `/root/...` as root, which `ProtectHome=read-only` blocks, so it would have started clean and been unable to write a frame |
| `17ea6da` | The spool was pinned with `Environment=`, which `EnvironmentFile=` overrides whatever the order — so `/etc/nmea2s3/env` had the last word on a path only the unit's sandbox permits. Now `--disk-dir`, which is not in that contest |
| `2ced7e1` | **`_log/` keys carry the application as a directory.** It was a body field only, so selecting or removing one tool's entries meant GETting every object — and lifecycle rules, which match a prefix and nothing else, could not target them at all |
| `98938e7` | **Item 5 above, all eight bullets.** Every stale claim in a module docstring, verified against the code rather than taken from the list: `object_exists()` described as removed when it is called by the importer; a function index with two wrong signatures and five missing entries; two references to a `MemoryMax` the unit has not had; a PyPI install line for something not on PyPI; a console script named `nmea2s3`; an index entry for a `__main__` wrapper that was deleted; `NMEA2S3_PG_PORT` listed as required when it defaults; and a range comment stating a convention its own bound contradicts. Also `--disk-dir`, which reached the CLI without reaching the docstring that lists the options |
| `9a1e3ef` | Seven review findings. The worst was mine from earlier the same day: `iter_log_keys()` crashed on every `_log/` key written before the application moved into it, from inside the generator and so outside the exporter's per-key `try` — one legacy object made the audit log unreadable by its own tool. Also: Ctrl-C recorded an interrupted import as FINISHED with exit 0; an empty `--disk-dir` fell through to the default; one Postgres transaction spanned the whole multi-hour import |
| `079121a` | 0.3.0, and the version now has one home. It lived in both `pyproject.toml` and `__init__.py`, and `--version` prints the module's copy while pip installs pyproject's — so the number the journal reports could drift from the number that was installed, which defeats the only reason to have it. `v0.3.0` tagged |
| `0c44842` | The 0183 importer moved into this repo as `nmea2s3-migrate-n0183`. It was converting a naive `received_at` with `.astimezone()`, which assumes the local zone rather than failing — a `timestamp without time zone` column imported from a Sydney-set box filed every row ten hours early, invisibly and permanently. Now refused |
| `319558b` | **Item 1 above.** Every temperature field id converts to Celsius, not just the deprecated PGN's. `ranges.py` and the `temp_sea` chain follow. **Carries a migration obligation** the retired item used to hold: rebuild the WHOLE range in one go, or the converted columns mix Kelvin and Celsius under one name — and since `temp_sea` now prefers 130316, a partial rebuild reads worse than before. Recorded in `README.md` rather than left in a deleted TODO entry |
| `65235f3` | **Item 2 above.** The start audit entry was awaited before the listener existed: out of coverage at power-up, up to 60s in which the one process that cannot re-read its input captured nothing |

---

## Done — 2026-08-30

Recorded so they are not re-litigated. All four pushed, 106 tests passing.

| commit | what |
|---|---|
| `353fc0e` | `table.ensure()` resolved the column lookup by bare name across every schema, so a same-named table elsewhere suppressed an `ALTER` and the COPY failed with `column does not exist` — the exact failure that function exists to prevent. Now resolves through `to_regclass`, the same relation the DDL touches, with `ADD COLUMN IF NOT EXISTS` as a backstop |
| `b6a94fd` | `SCHEMA.md` and `logger.py` claimed the bad-clock filing window was "closed by" the unit ordering after `time-sync.target`. `After=` is ordering only and nothing pulls that target in — correctly, since the service that would blocks forever offshore. Docs now say capture does not wait and does not judge; the false claim was also reason 2 of 3 for deleting `n2k_quarantine/`, replaced with the true one |
| `8dd223e` | The GPS clock reached no column from either protocol — `date`/`time` come back as datetime objects and the numeric filter dropped them, so PGN 126992, whose entire purpose is a clock, contributed nothing. Now `gps_time` in POSIX seconds from 129029, GPS-sourced 126992 and RMC, COALESCEd in `metrics_1s` so `ts` can be checked against it in SQL |
| `ac7cb5c` | LOOKUP fields were dropped for the same reason, taking `method` (GNSS fix quality) with them. Now stored as their code, `_code`-suffixed; the five frame-metadata lookups that head almost every proprietary PGN are skipped by name |
