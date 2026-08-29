# nmea2s3

Captures raw NMEA 2000 traffic off a boat's CAN bus and archives it in an
S3-compatible bucket, batched and gzip'd, with a local spool that survives an
outage of any length the disk can hold.

```
 CAN bus ──► nmea2s3-logger ──► spool file ──► S3
          (SocketCAN)        (local disk)   (the durable copy)
```

It decodes nothing. A frame arrives, gets a kernel timestamp and a monotonic
reading, becomes one line of ndjson, and is written somewhere durable. Every
protocol is stored in the same six-field record, so NMEA 2000 and NMEA 0183
land in one archive on one timeline. The record shape, key layout and audit
log are documented in full in `SCHEMA.md`.

## Install

```bash
pipx install git+https://github.com/ghotihook/nmea2s3.git
```

Not on PyPI. Pin a tag for anything you actually deploy, so
`nmea2s3-logger --version` in the journal tells you truthfully what is running.
To update, re-run the install with `--force` — a plain `pipx install` of an
already-installed package is a no-op, and `pipx upgrade` only acts when the
version number has gone up, so a commit that forgot to bump `version` would
otherwise never reach you, at the one step that cannot be re-run.

Three commands land on your `PATH`:

| command | does |
|---|---|
| `nmea2s3-logger` | the logger — SocketCAN to S3, meant to run under systemd |
| `nmea2s3-exporter` | read the archive back out as ndjson, CSV or candump |
| `nmea2s3-update-pg` | archive → a wide Postgres table |

Python 3.10+; the logger needs Linux (SocketCAN), the other two run anywhere.
The logger imports only boto3 and the stdlib — the decoders and the database
driver are not in the capture process.

## Configure

`pipx` installs the commands, not the repo, so fetch the two files a deployment
needs:

```bash
curl -O https://raw.githubusercontent.com/ghotihook/nmea2s3/main/env.example
curl -O https://raw.githubusercontent.com/ghotihook/nmea2s3/main/systemd/nmea2s3.service
cp env.example env       # then fill in the four required values
```

Four variables are required — endpoint, bucket, and a key pair; the rest are
documented in `env.example`. **Give the logger a credential that can PUT and
cannot DELETE** — nothing here ever removes an object.

The file is plain `KEY=value` with **no `export`** (systemd's `EnvironmentFile=`
rejects any line that carries one). In a shell a bare `source env` does not
export, so:

```bash
set -a; source env; set +a
nmea2s3-logger --can can0
```

## Run it

```bash
nmea2s3-logger --can can0            # the interface, default can0
nmea2s3-logger --log-level DEBUG     # or $NMEA2S3_LOG_LEVEL
nmea2s3-logger --device-id test      # what goes in each row's device_id
```

For a real deployment, use the `nmea2s3.service` you fetched above — a template
with three placeholders, the reasoning for every directive written inline. It is
worth reading before you copy it:

```bash
sudo install -Dm600 env /etc/nmea2s3/env
sudo install -m644 nmea2s3.service /etc/systemd/system/
sudo sed -i "s/youruser/$USER/g" /etc/systemd/system/nmea2s3.service
sudo systemctl daemon-reload && sudo systemctl enable --now nmea2s3
journalctl -u nmea2s3 -f
```

## Is it working?

The first object takes **up to five minutes** to appear — a flush moves the
buffer to a spool file every 5 minutes and a separate loop uploads every 30
seconds. An empty bucket a minute after starting is normal, not a fault.

Meanwhile the logger prints a line a minute:

```
[n2k] rx=12483 spooled=12000 objects=2 dropped=0 no_kernel_ts=0 bad_clock=0
      buf=483 (79695 bytes) spool=0 file(s) epoch=1755000000.123
```

- `rx` climbing means frames are arriving; stuck at 0 means the CAN interface
  is down or wrong (`ip link show can0`).
- `spool` is files written but not yet uploaded — a value that keeps growing
  means S3 is unreachable, which is survivable and expected offshore.
- `dropped` should stay 0. Non-zero means data was evicted and is gone.
- `epoch` is `ts - mono`; it should be constant. A jump is NTP correcting the
  clock.

Set `NMEA2S3_DISK_DIR` to a persistent mount with room to spare — never tmpfs.

## Read it back

```bash
nmea2s3-exporter | jq .                                    # everything, ndjson, to stdout
nmea2s3-exporter --proto n0183 --since 2026-08-01 -o wk.ndjson   # one protocol
nmea2s3-exporter --source _log --format csv -o             # audit log, auto-named file
nmea2s3-exporter --proto n2k --format candump | canboat convert   # decode with canboat
```

**Decoding the archive with canboat is one pipe** — that last line, usually a day
at a time:

```bash
nmea2s3-exporter --proto n2k --format candump --since 2026-08-28 --until 2026-08-28 \
  | canboat convert
```

`--format candump` writes the ASCII form `candump -L` writes and canboat reads;
it is CAN frames, so it implies `--proto n2k` and refuses `--source _log` or
another `--proto` at the flags. `--proto` filters on the object *name*, which a
`LIST` already returned, so a narrowed export downloads nothing it will discard.

Exit 2 means the export finished but is incomplete — at least one object could not
be read after retries, named on stderr.

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

The table builds itself — created if absent, and it gains a column the first time
a field appears (put a new instrument on the bus and its fields turn up as
columns). That is only safe because the database is derived and disposable:
everything in it rebuilds from the archive, so never point it at anything that
cannot be regenerated.

Ingest is **exactly-once by object** (content-addressed keys plus a ledger of
consumed keys), so it is safe to run on a cron over an overlapping window;
`--rebuild` reprocesses anyway, since every write is an upsert keyed on `ts`.

Two layers, and the names say which is which:

| | | |
|---|---|---|
| `observations` | table | one column per decoded field, every instrument kept — what was **reported** |
| `observations_objects` | table | the ledger of ingested keys |
| **`metrics_1s`** | view | instruments resolved into named quantities — what you **query** |

`sql/metrics.sql` is an example to adapt — it names fields your archive may not
carry, so delete the chain entries you do not have; it also grants `SELECT` on
the view to `ro_user` for dashboards.

## How it works

The short version: spool first, always — no online/offline mode, an unreachable
bucket just means a file is still on disk next pass; content-addressed keys, so a
retry is a harmless overwrite, never a duplicate; nothing is decoded on the way
in, so a decoder bug is a fix and a re-derive, not a permanent correction; and
every frame carries two clocks (`ts` and `mono`) so a later NTP correction is
detectable without anything here judging it.

`SCHEMA.md` has the full record shape, key format, audit log and the reasoning
behind each; `tests/test_formats.py` checks the code against it, so the two
cannot drift.

## Tests

```bash
python tests/run.py              # everything, no dependencies beyond boto3
python tests/run.py logger gzip  # only modules matching these names
pytest tests/                    # same tests, nicer failure output
```

Nothing touches a network or the real bucket; it runs against the working tree,
so it works on the boat with nothing installed but the logger's own
dependencies.

## Licence and trademarks

MIT — see `LICENSE`.

NMEA 2000® and NMEA 0183® are trademarks of the National Marine Electronics
Association. This project is an independent open-source tool, not affiliated
with, endorsed by, or certified by the NMEA.
