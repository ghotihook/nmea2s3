# TODO

Known issues and deferred decisions, newest review 2026-08-30. Nothing here
is a guess: every entry names the file and line it lives at, and says how it
was established — read, measured, or reproduced.

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

### 1. Sea temperature is stored in two different units

`src/nmea2s3/pg/wire_n2k.py:53` — `CONVERSIONS` keys on the decoder's field
id and lists only `actualTemperature`. The library names the same quantity
differently in other PGNs, so no conversion fires and Kelvin goes into the
column. Reproduced — the same 20.00 °C reported three ways:

```
130312  n2k_actualtemperature_0_sea_temperature      20.0     degC
130316  n2k_temperature_0_sea_temperature           293.15    KELVIN
130310  n2k_watertemperature                        293.15    KELVIN
        n2k_settemperature_0_sea_temperature        293.15    KELVIN
```

130316 is the *modern* PGN; 130312 is deprecated in favour of it, so a
current instrument pack is the case that goes wrong. `ranges.py:47` bounds
only the Celsius column `(0.0, 40.0)`, so the Kelvin ones are unguarded —
and were that bound ever applied to one, every reading would be dropped as
impossible. Today `metrics_1s.temp_sea` chains only the Celsius column, so a
130316 boat silently falls through to `mda_water_temp`.

Fix: four keys in `CONVERSIONS` (`temperature`, `setTemperature`,
`waterTemperature`, `outsideAmbientAirTemperature`), matching `ranges.py`
entries, and the `temp_sea` chain in `sql/metrics.sql`.

**Blocks the rebuild.** This changes values in a column that already exists,
under the same name. Rebuilding part of a range after fixing it leaves that
column holding Kelvin for old rows and Celsius for new ones, with nothing to
tell them apart. Fix first, then rebuild once over the whole range.

### 3. A partial write can duplicate rows into the archive

`src/nmea2s3/logger.py:669-681` — `_objects()` splits by capture day, so a
flush spanning UTC midnight yields two objects. If `_write_spool` places
object 1 and fails on object 2 it returns `False`; `_upload_objects` then
re-sends both, and on a part-way failure `_return_buffer(batch)` returns the
*whole* batch to RAM including frames already durable. The next flush regroups
them with newer arrivals, producing a different content id and so a different
key.

The only place the "identical rows always resolve to the same key, so a retry
is a harmless overwrite" invariant does not hold. `_write_spool` guards it
(`if path.exists(): continue`, :827); `_upload_objects` and `_return_buffer`
have no equivalent. Needs a midnight-spanning flush *and* a partial failure,
so it is narrow — but either track which objects were placed and return only
the rest, or document that this path can duplicate.

---

## P1 — correct data, misleading answers

### 4. `sql/metrics.sql` assumes the table is in `public`

`GRANT USAGE ON SCHEMA public` and an unqualified `FROM observations`, while
`nmea2s3-update-pg` writes to wherever `search_path` resolves — which nothing
in the repo pins. The last remaining schema assumption after `353fc0e` fixed
the same class of bug in `table.ensure()`.

### 5. Stale claims in module docs

Each verified against the code:

- `src/nmea2s3/ndjson.py:11-18` says `object_exists()` "lived here until
  2026-08-28 and was called by nothing." It is still at `:266`, still called
  by nothing, and its docstring justifies it by "a day-at-a-time importer"
  that does not exist in this repo. It is also the only caller of
  `with_retries(quiet_codes=…)`, so removing it makes that parameter dead too.
- `src/nmea2s3/ndjson.py:27,32` — the function index gives
  `s3_key(source, day, …)` building `<source>/<yyyy>/…` (actual: `s3_key(proto,
  …)` → `raw/…/<HHMMSS>-<proto>-<cid>.ndjson.gz`) and `iter_keys(s3, bucket,
  source, since, until)` (actual: `(s3_client, bucket, since, until,
  proto=None)`). `record_line`, `valid_proto`, `key_proto` and `iter_log_keys`
  are absent from the index — including the one function that defines the record.
- `src/nmea2s3/logger.py:206` — "the unit's `MemoryMax=128M`". The unit is
  512M, and `:221` in the same comment block reasons correctly from 512M.
- `src/nmea2s3/logger.py:130` — "Install: `pipx install nmea2s3`". Not on
  PyPI; `README.md:24` installs from git.
- `src/nmea2s3/logger.py:919` — "Console-script entry point — `nmea2s3`".
  The script is `nmea2s3-logger`.
- `src/nmea2s3/audit_log.py:14-15,53-55` — describes start/end entries "paired
  by a `run_id` in `details`". No tool writes a `run_id`, and `SCHEMA.md:222`
  says the opposite explicitly. `:40` also carries an orphaned bullet line
  describing the `__main__` wrapper the docstring itself says was removed.
- `src/nmea2s3/pg/update.py:65` lists `NMEA2S3_PG_PORT` as required;
  `load_config():117` defaults it to 5432, and `env.example` has it optional.
- `src/nmea2s3/pg/ranges.py:26` says signed fields are "folded to (-180, 180]";
  the bound at `:40` is `(-180.0, 360.0)`. If the wide bound is deliberate
  hedging against the `UNVERIFIED` note below it, the comment above should not
  state the narrow convention as fact.

### 6. `--table` cannot be schema-qualified

`table.py:33` `SAFE_NAME` rejects a `.`, so `--table analytics.observations`
fails with a message about decoder field ids. One line in the flag's help.

---

## P2 — foreseeable conditions that fail confusingly

### 7. A malformed key aborts the whole export

`src/nmea2s3/ndjson.py:246,262` — `key_date` and `key_proto` raise
`ValueError` on any object under `raw/` that does not match the layout, from
inside the `iter_keys` generator. That is consumed by the `for key in keys:`
statement in `export_source`, *outside* the per-object `try`, so one stray
object defeats the skip-and-continue design at `export.py:239-269`.

### 8. `sql/` is unreachable after the documented install

`README.md:205` says `psql "$PG" -f sql/metrics.sql`, but `:44` correctly
notes "pipx installs the commands, not the repo" and gives `curl` lines for
`env.example` and the unit file only. `sql/` is also missing from
`[tool.hatch.build.targets.sdist].include` (`pyproject.toml:58`), which does
list `tests` and `systemd`. Add a third `curl` line and `sql` to the include.

### 9. Two names for the connection string, neither defined

`README.md:205` uses `$PG`; `sql/metrics.sql:19` uses `$DATABASE_URL`.
`env.example` defines neither — it exposes five discrete `NMEA2S3_PG_*` vars,
which `psql` does not read. Pick one and show how to build it.

### 10. Export's atomic write drops the fourth step

`src/nmea2s3/export.py:279-283` — file fsync then rename, no directory fsync,
while `:222-227` claims "same tmp-then-rename reasoning as the disk spool",
whose own `_fsync_dir()` docstring (`logger.py:789-803`) calls that step "the
easiest one to leave out". Low stakes — an export re-runs — but the comment
claims a parity the code does not have.

### 11. `n2k_spare` reaches a column

`src/nmea2s3/pg/wire_n2k.py:87` filters `"reserved" in f.id.lower()`, but
SPARE is a separate field type and is not caught. One word in the condition.
Observed on PGN 60928.

### 12. ISO Address Claim fills columns with bus-management metadata

60928 contributes `n2k_uniquenumber`, `n2k_deviceinstancelower`,
`n2k_deviceinstanceupper`, `n2k_systeminstance` and friends — plain NUMBERs,
so they predate the lookup work; `ac7cb5c` added three `_code` columns to the
same PGN. If address-claim traffic clutters the table the fix is a PGN-level
skip, not a field-level one. Deliberately not done: no evidence yet that it
is a problem in practice.

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

### 14. `ensure()` is called twice per object

`pg/update.py:220` and again inside `table.write()` (`table.py:96`), so every
object costs two `CREATE TABLE IF NOT EXISTS` + catalogue round trips. Having
`write()` return the columns it added would drop half. Only reason for the
outer call is capturing `added` for the run summary.

### 15. BITLOOKUP and INDIRECT_LOOKUP are still dropped

`src/nmea2s3/pg/wire_n2k.py:34` — deliberate. A bitfield wants a mask column,
not a code; an indirect lookup means nothing without the field it depends on.
Both need a naming rule of their own, so they wait for a use.

### 16. Wind-angle convention unverified against real traffic

`src/nmea2s3/pg/ranges.py:28-32` — which convention the `nmea2000` library
returns for PGN 130306 has not been confirmed. The wire format is 0..2π,
which would make those fields bearings rather than signed angles. Check the
first real day where the n2k and 0183 streams overlap. Related to the
comment/bound mismatch in item 5.

### 17. A batch larger than the whole spool budget is written anyway

`src/nmea2s3/logger.py:830-838` — no truncation implemented; eviction clears
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

---

## Done — 2026-09-03

| commit | what |
|---|---|
| `7b0aff5` | The unit ran as an unprivileged user from `~/.local/bin`. Now root from a `pipx --global` install, spooling to `/var/lib/nmea2s3` — the default `Path.home()/n2k_fallback` is `/root/...` as root, which `ProtectHome=read-only` blocks, so it would have started clean and been unable to write a frame |
| `17ea6da` | The spool was pinned with `Environment=`, which `EnvironmentFile=` overrides whatever the order — so `/etc/nmea2s3/env` had the last word on a path only the unit's sandbox permits. Now `--disk-dir`, which is not in that contest |
| `2ced7e1` | **`_log/` keys carry the application as a directory.** It was a body field only, so selecting or removing one tool's entries meant GETting every object — and lifecycle rules, which match a prefix and nothing else, could not target them at all |
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
