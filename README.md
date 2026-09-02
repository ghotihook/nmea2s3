# nmea2s3

Captures raw NMEA 2000 traffic off a boat's CAN bus and archives it in an
S3-compatible bucket, batched and gzip'd, with a local spool that survives an
outage of any length the disk can hold.

```
 CAN bus ──► nmea2s3-logger ──► spool file ──► S3
             (SocketCAN)        (local disk)   (the durable copy)
```

It decodes nothing and interprets nothing. A frame arrives, gets a kernel
timestamp and a monotonic reading, becomes one line of ndjson, and is written
somewhere durable. That is the entire job: capture is the one step that
cannot be re-run, so it is the one step that carries no cleverness.

Every protocol is stored in the **same six-field record**, so NMEA 2000 and
NMEA 0183 land in one archive on one timeline, and a reader dispatches on a
field rather than on which directory a row came from.

## Install

```bash
sudo pipx install --global --force git+https://github.com/ghotihook/nmea2s3.git
```

Not on PyPI — install from the repo. `--global` builds the venv in
`/opt/pipx/venvs/nmea2s3` and symlinks the commands into `/usr/local/bin`,
which is where the systemd unit expects them; without it they land in
`~/.local/bin` and `ExecStart=` has to be pointed there instead.

**`--force` on every install, upgrades included.** `pipx upgrade` compares
version numbers, so new commits that did not bump `version` in
`pyproject.toml` look like nothing to do — leaving you on old code believing
you upgraded, at the one thing that cannot be re-run. An upgrade replaces the
venv underneath the running process, so follow it with
`sudo systemctl restart nmea2s3`.

Pin a tag for anything you actually deploy, so `nmea2s3-logger --version` in
the journal tells you truthfully what is running:

```bash
sudo pipx install --global --force git+https://github.com/ghotihook/nmea2s3.git@v0.1.0
```

Three commands land on your `PATH`:

| command | does |
|---|---|
| `nmea2s3-logger` | the logger — SocketCAN to S3, meant to run under systemd |
| `nmea2s3-exporter` | read the archive back out as ndjson or CSV |
| `nmea2s3-update-pg` | archive → a wide Postgres table |

Python 3.10+, Linux for the logger (SocketCAN); the other two run anywhere.
Dependencies are boto3 for the logger and exporter, and psycopg, nmea2000
and pynmea2 for the Postgres side.

**The logger imports boto3 and the stdlib, and nothing else** — not the
decoders, not the database driver. Installing them alongside it does not put
them in the capture process; that stays as small as it ever was, which is
the property that matters for the one service that cannot re-run a missed
frame. The cost is at install time, and it lands on the boat: an SBC needs a
wheel for its platform for each dependency, or a compiler to build one.

## Configure

`pipx` installs the commands, not the repo, so fetch the two files a
deployment needs:

```bash
curl -O https://raw.githubusercontent.com/ghotihook/nmea2s3/main/env.example
curl -O https://raw.githubusercontent.com/ghotihook/nmea2s3/main/systemd/nmea2s3.service
cp env.example env       # then fill in the four required values
```

Four variables are required — endpoint, bucket, and a key pair; the rest are
documented in `env.example`. **Give the logger a credential that can PUT and
cannot DELETE.** The archive is the source of truth and nothing here ever
removes an object, so a key that cannot delete turns an accident from
unlikely into impossible.

**Loading the file.** It is plain `KEY=value` lines with **no `export`**,
because systemd's `EnvironmentFile=` parser rejects any line that carries one
— a warning per line in the journal and a service that starts with none of
its environment. That format costs one thing in a shell: a bare
`source env` sets shell variables without exporting them, so the commands you
then run see nothing. Wrap it in `set -a`:

```bash
set -a; source env; set +a     # the commands below read the environment
nmea2s3-logger --can can0
```

Under systemd nothing needs sourcing — the unit points `EnvironmentFile=` at
a copy installed mode 600 under `/etc` (below), which systemd reads before
starting the process. The service runs as root, so keep that copy root-owned
and 600: nothing else on the box has any reason to read the key.

## Run it

```bash
nmea2s3-logger --can can0            # the interface, default can0
nmea2s3-logger --log-level DEBUG     # or $NMEA2S3_LOG_LEVEL
nmea2s3-logger --device-id test      # what goes in each row's device_id; default hostname
```

For a real deployment, use the `nmea2s3.service` you fetched above. Nothing
in it needs editing for a `--global` install — it runs as root, spools to
`/var/lib/nmea2s3`, and every path is already absolute. It is worth reading
before you copy it: the
ordering, the restart policy and the memory limit are each there because of
a specific way this has failed before.

```bash
sudo install -Dm600 env /etc/nmea2s3/env
sudo install -m644 nmea2s3.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now nmea2s3
journalctl -u nmea2s3 -f
```

## Is it working?

The first object takes **up to five minutes** to appear. A flush moves the
buffer to a spool file every 5 minutes and a separate loop uploads spool
files every 30 seconds, so an empty bucket a minute after starting is normal,
not a fault. The interval is tuned for archive object size, not freshness —
this is a cold path.

Meanwhile the logger prints a line a minute:

```
[n2k] rx=12483 spooled=12000 objects=2 dropped=0 no_kernel_ts=0 bad_clock=0
      buf=483 (79695 bytes) spool=0 file(s) epoch=1755000000.123
```

- `rx` climbing means frames are arriving; stuck at 0 means the CAN
  interface is down or wrong (`ip link show can0`).
- `spool` is files written but not yet uploaded — a non-zero value that keeps
  growing means S3 is unreachable, which is survivable and expected offshore.
- `dropped` should stay 0. Non-zero means data was evicted and is gone.
- `epoch` is `ts - mono`; it should be constant. A jump is NTP correcting the
  clock. Nothing waits for NTP — an RTC-less box that boots with a wrong
  clock captures happily and files those first objects under the day it
  believed, and a content-addressed key cannot be rewritten. Fit a hardware
  RTC if that matters to you; short of one, `mono` on every frame makes the
  jump visible after the fact, and `nmea2s3-update-pg` stores the GPS clock
  beside `ts` as `gps_time` — from N2K and 0183 alike — so a row stamped by
  a wrong clock is one predicate away. Nothing anywhere drops a row over it.

The spool needs a persistent mount with room to spare — never tmpfs. It holds
2 GB, roughly 6 days at a busy bus's frame rate, before it starts dropping its
oldest batch. Under systemd it is `/var/lib/nmea2s3`, created by the unit's
`StateDirectory=` and passed on the command line as `--disk-dir`. The flag is
deliberate: `EnvironmentFile=` always overrides `Environment=`, so a unit
cannot pin the spool with an environment variable — the env file would get the
last word and could point it somewhere the sandbox refuses to let it write.
The command line is not in that contest, so `NMEA2S3_DISK_DIR` in the env file
is simply ignored rather than obeyed-and-broken. To put the spool elsewhere —
a USB stick, if SD/eMMC wear is a concern — change `--disk-dir` in the unit
and add a matching `ReadWritePaths=`. Run by hand, with no flag, it falls back
to `$NMEA2S3_DISK_DIR` and then `~/n2k_fallback`.

## Read it back

```bash
nmea2s3-exporter | jq .                                    # everything, ndjson, to stdout
nmea2s3-exporter --proto n0183 --since 2026-08-01 -o wk.ndjson   # one protocol
nmea2s3-exporter --source _log --format csv -o             # audit log, auto-named file
nmea2s3-exporter --proto n2k --format candump | canboat convert   # decode with canboat
```

**Decoding the archive with canboat is one pipe** — that last line is the
whole of it, and a day at a time is usually what you want:

```bash
nmea2s3-exporter --proto n2k --format candump --since 2026-08-28 --until 2026-08-28 \
  | canboat convert
```

`--format candump` writes the ASCII form `candump -L` writes and canboat
reads:

```
(1502979132.106111) slcan0 09F50374#000A00FFFF00FFFF
```

Nothing here decodes anything — `raw` already *is* `<canid>#<data>`, so the
exporter re-cases it, pads the identifier to the full 29-bit width and puts
the timestamp back in the shape candump wrote it in. What a PGN *means* stays
canboat's business, which is the same reason the archive stores no `pgn`
column: today's decoder never gets frozen into objects that are never
rewritten.

It is CAN frames, so it implies `--proto n2k` — 0183 sentences carry no CAN
identifier and the format has no way to say so. Passing `--proto n2k`
explicitly, as above, says the same thing out loud. `--source _log` or
`--proto n0183` alongside it are refused at the flags rather than quietly
exporting nothing.

`--proto` filters on the object *name*, which a `LIST` already returned, so a
narrowed export downloads nothing it will discard. That matters more than it
sounds: n2k outruns n0183 by roughly 660:1, so filtering after download would
mean fetching ~45 GB to extract ~0.07 GB of sentences.

Because every protocol shares one record, a mixed export is a well-formed
single file — including CSV, which the old per-protocol column sets made
impossible.

Exit 2 means the export is incomplete — either at least one object could
not be read after retries (each named on stderr), or the reader of the
output closed the pipe before the export finished.

## Query it in Postgres

`nmea2s3-update-pg` turns the archive into a wide table — one row per time
bucket, one column per decoded field:

```bash
nmea2s3-update-pg                                     # 1 s buckets into `observations`
nmea2s3-update-pg --bucket 5m --table observations_5m # any bucket: 250ms, 1s, 5m, 1h
nmea2s3-update-pg --proto n2k --since 2026-08-01
nmea2s3-update-pg --dry-run -v                        # decode and report, write nothing
psql "$PG" -f sql/metrics.sql                        # the views you actually query
```

```
 ts                     | n2k_sog | n2k_windangle_apparent | mwv_wind_angle_r
------------------------+---------+------------------------+------------------
 2026-08-24 12:00:00+00 |    6.41 |                 -34.20 |           -33.90
 2026-08-24 12:00:01+00 |    6.38 |                 -35.10 |
```

**The table builds itself.** It is created if absent, and gains a column the
first time a field appears — put a new instrument on the bus and its fields
turn up as columns on the next run, NULL for the rows that predate it. That
is only safe because this database is derived and disposable; everything in
it rebuilds from the archive.

**A lookup is stored as its code.** N2K carries enumerated fields — GNSS fix
quality, integrity, reference type — whose meanings live in a table. The
column holds the number (`n2k_method_code` = 2), never the resolved text: it
is what the instrument actually said, it fits one column type, and it leaves
the enum where a wrong entry is a fix rather than something frozen into rows
that are never rewritten. Spell it out in SQL. The lookups that describe the
*frame* rather than a measurement — `manufacturerCode`, `industryCode` and
three more — are skipped, since between them they head almost every
proprietary PGN and a column each would bury the ones worth having.

**Columns are `proto_field`, one per decoded field.** `n2k_sog`,
`rmc_spd_over_grnd` and `vtg_spd_over_grnd_kts` are three columns, not one.
Nothing picks a winner *between columns* — which instrument you trust is a
question about your boat, it changes, and it is answerable in SQL over these
rows. A value discarded at write time is not.

**Two devices reporting the same field share one column**, so there one of
them has to win. The bucket picks by **lowest N2K priority number, then
lowest source address, then the latest sample** — the device first, then that
device's last reading. Priority is set by the sending device's firmware and
travels with it; addresses are leased by ISO address claiming and change when
the bus is repowered, so they rank devices without naming any. The cost is
bounded staleness: a priority-1 device reporting at 0.2 Hz wins the buckets
it appears in, and no others — nothing carries across a bucket boundary, so a
device that goes quiet loses the next bucket outright.

**Each bucket takes `last()`**, a real reading, not a mean. A mean
would need to know which fields are angles (the mean of 359° and 1° is 180°),
and this table adds columns for fields nobody has declared. Take a mean in
SQL, where you can say which columns are bearings.

**The GPS clock is a column, so `ts` can be checked.** `ts` is the kernel's
capture time — `CLOCK_REALTIME`, only as good as the logger's own clock was.
Both protocols carry an independent UTC reference (N2K PGNs 129029 and
126992, and 0183 RMC), and all of them resolve to one `gps_time` in
`metrics_1s`. Nothing compares the two for you, because how far apart is too
far is a question about what you are asking:

```sql
SELECT * FROM metrics_1s
 WHERE gps_time IS NOT NULL
   AND abs(extract(epoch FROM ts) - gps_time) < 2;
```

A small *positive* offset is normal — the frame is stamped on receipt, one
transmission time after the fix it reports. `gps_time IS NULL` means no GPS
reported into that bucket, which is not the same finding as a disagreement.
The foot of `sql/metrics.sql` has both queries and the reasoning.

Ingest is **exactly-once by object**: keys are content-addressed, so a
ledger table of consumed keys makes it safe to run on a cron over an
overlapping window. `--rebuild` reprocesses anyway — every write is an
upsert keyed on `ts`.

### Two layers, and the names say which is which

| | | |
|---|---|---|
| `observations` | table | one column per decoded field, every instrument kept — what was **reported** |
| `observations_objects` | table | the ledger of ingested keys |
| **`metrics_1s`** | view | instruments resolved into named quantities — what you **query** |

The tables are written by `nmea2s3-update-pg`; the view comes from
`sql/metrics.sql`, which is an example to adapt — it names fields your
archive may not carry, so delete the chain entries you do not have. It also
grants `SELECT` on the view to `ro_user`, and on nothing else: a view runs
its query as its owner, so a dashboard reads the resolved layer without any
privilege on the raw per-instrument table.

`metrics_1s` resolves the instrument chains with `COALESCE`: the first
instrument that reported a bucket wins and contributes its own value, never
an average across a chain — two instruments differing by a known offset must
not be blended into a number neither of them measured.

**If you resample coarser, angles need a circular mean.** The arithmetic mean
of 359° and 1° is 180°, a heading pointing exactly backwards, produced
silently from two readings two degrees apart. `sql/metrics.sql` ends with the
recipe, verified against PostgreSQL 14 — that case returns 0° where `avg()`
returns 180°.

## The parts worth knowing

**Spool first, always.** Every flush writes the buffer to a local file
(tmp → fsync → rename → fsync the directory) and a separate loop uploads
from there and deletes each file as it lands. There is no online mode and no
offline mode, no reachability probe, no recovery routine — an unreachable
bucket just means a file is still sitting there on the next pass. That is
the entire outage story, and it is why there is one write path instead of
the three an online/disk/recovery split needed.

**One tree, content-addressed keys.**
`raw/<yyyy>/<mm>/<dd>/<HHMMSS>-<proto>-<id>.ndjson.gz`. Date first, so
"everything captured on this day" is a single prefix; protocol in the object
name, so a single-protocol read filters before it downloads. The id is a hash
of the pre-gzip text and the date is when the frames were *captured*, never
when they were uploaded — so identical rows always resolve to the same key
with byte-identical contents, a retry is a harmless overwrite rather than a
duplicate, and a batch delayed three days by an outage still files itself
under the day it was recorded.

**Nothing is decoded on the way in.** `pgn`, `src_addr`, `priority` and an
0183 sentence type are all pure functions of the bytes already stored, so
storing them too cost ~17% of every object and froze one particular decoder
into an archive that is never rewritten. `nmea2s3.decode` computes them on
read instead:

```python
from nmea2s3 import decode
decode.n2k("09f80102#a54dca182530bb1d").pgn        # 129025
decode.n0183("$GPRMC,...*6A").sentence_type        # 'RMC'
```

**Two clocks on every frame.** `ts` is the kernel's `SO_TIMESTAMPNS`, stamped
in the driver's softirq so it carries no scheduler latency — but it is
`CLOCK_REALTIME`, only as correct as the system clock was at the time. An
RTC-less SBC boots with a wrong clock and learns the truth when NTP first
syncs. So every frame also carries `mono`, `CLOCK_MONOTONIC`, which cannot
jump: `ts - mono` is the boot epoch, constant while the clock is stable and
shifted by exactly the step when NTP corrects it. Nothing is repaired and
nothing is judged — the logger records both readings and lets whatever reads
the archive decide.

**Both buffers shed their oldest end.** RAM is capped at 48 MB of serialized
rows and the spool at 2 GB; past either, the oldest data goes first. Losing
the oldest not-yet-written frames beats losing the newest, and either beats
an OOM kill that loses the entire buffer at once.

`SCHEMA.md` documents the record shapes, the key format and the `_log/`
audit log in full, and `tests/test_formats.py` checks the code against it,
so the two cannot drift.

## Tests

```bash
python tests/run.py              # everything, no dependencies beyond boto3
python tests/run.py logger gzip  # only modules matching these names
pytest tests/                    # same tests, nicer failure output
```

Nothing in the suite touches a network or the real bucket. It runs against
the working tree, so it works on the boat over a slow link with nothing
installed but the logger's own dependencies — which is the point, because
that is where you find out something is wrong.

## Provenance

Carved out of a larger private flight-recorder project, where this ran as
`flightrecorder_logger`. Most of the tests here are regressions: each one
pins down something that was once actually wrong, and says which in its
docstring. `_log/` entries written before the split are filed under the old
name.

## Licence and trademarks

MIT — see `LICENSE`.

NMEA 2000® and NMEA 0183® are trademarks of the National Marine Electronics
Association. This project is an independent open-source tool: it is not
affiliated with, endorsed by, or certified by the NMEA, and the standards are
named here only to describe what it reads.
