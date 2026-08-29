# Raw record schema (S3)

What `nmea2s3-logger` writes into the bucket, and what `nmea2s3-exporter` reads back
out.

**One record shape for every protocol.** An NMEA 2000 frame and an NMEA 0183
sentence are the same kind of thing — some bytes that arrived on a wire at a
known moment — and they are stored identically. A reader dispatches on each
row's `proto` field; nothing downstream needs to know which prefix a row came
from, or to union two different column sets to build one timeline.

`src/nmea2s3/ndjson.py` implements everything below — the record, the key
format, hashing, gzip, upload and read-back. One implementation, so a second
copy cannot drift away from this document. `tests/test_formats.py` parses the
tables here and checks them against what the code actually writes, which is
what keeps that promise honest rather than aspirational.

## Conventions

- **`ContentType: application/gzip`, no `ContentEncoding` header.** Every
  writer uploads with `ContentType="application/gzip"` and deliberately
  omits `ContentEncoding: gzip`. That header tells HTTP-aware clients
  (browsers, some CLI tools) to transparently decompress on download —
  which would silently leave plain ndjson on disk under a `.gz` filename.
  The object *is* the gzip archive, not a gzip-transported copy of
  something else.
- **UTC only, strictly enforced.** ISO 8601 with an explicit `+00:00`
  offset, e.g. `"2026-08-12T13:31:23.010581+00:00"`. `record_line()` raises
  on any non-UTC-aware timestamp — every construction path already produces
  one, so a failure means a code bug, not a data condition. Matches
  `CLOCK_REALTIME`'s own UTC-internal representation, so this is an
  invariant to preserve rather than a conversion to perform.
- **Missing values are `null`**, never omitted and never invented. `mono`
  is null for a capture path with no monotonic clock to read; `src` is null
  where the input has no meaningful name. A fabricated `mono` would
  silently corrupt `ts - mono`, which is the only reason the field exists.
- **Nothing is decoded.** `raw` is the frame or sentence exactly as it
  arrived. See [Deriving what isn't stored](#deriving-what-isnt-stored).
- **`ts` is the kernel's RX timestamp** (`SO_TIMESTAMPNS` for CAN), not
  `now()`. It is `CLOCK_REALTIME`, so it can be wrong before the first NTP
  sync and can step mid-run. Both are made recoverable by `mono` rather
  than repaired; the logger never judges a timestamp. A `bad_clock` counter
  flags anything before `TIMESTAMP_FLOOR`, and `no_kernel_ts` counts frames
  the kernel gave no RX timestamp for, which fall back to `now()`.
- **S3 key** — one tree, date first, protocol in the object *name*:
  ```
  raw/<yyyy>/<mm>/<dd>/<HHMMSS>-<proto>-<content_id>.ndjson.gz
  ```
  That combination does two jobs at once:
  - **"everything captured on this day" is a single prefix**, which is what
    a reader consuming the archive actually asks for. Splitting the
    protocols into separate directories would make that reader merge one
    listing per protocol for no benefit.
  - **"only this protocol, over a long range" stays cheap**, because `LIST`
    returns names without bodies. A reader filters `-<proto>-` on the key
    string and issues GETs for nothing else. Merging the protocols into one
    object *stream* would make that impossible: n2k outruns n0183 by about
    660:1, so a year of 0183 would mean downloading ~45 GB to extract
    ~0.07 GB.

  Each object holds exactly one protocol — that is what makes the name
  meaningful and the filtering possible.

  - `yyyy/mm/dd`: the rows' own UTC day. `group_by_day()` splits any write
    into per-day groups first, so a replay delayed by an outage still lands
    under the day it was captured, not the day it was uploaded.
  - `HHMMSS`: the first row's UTC time-of-day — human-readable only, no
    correctness weight.
  - `proto`: matches the `proto` on every row in the object. Deliberately
    redundant, so a row stays self-describing once separated from its key;
    a test checks the two agree, since redundancy that is never checked is
    just two things free to disagree.
  - `content_id`: `sha256(pre-gzip ndjson text)[:16]` — hashed before gzip,
    never after, since gzip's header timestamp would make identical rows
    hash differently on every retry. This is what makes retries idempotent:
    the same rows always resolve to the same key, so a retry overwrites
    harmlessly instead of duplicating. The writer also sets `MTIME=0`, so
    identical content produces byte-identical objects, verifiable by ETag
    without downloading or decompressing them.

  **No `device_id` and no `src` anywhere in the key.** Which physical box a
  row came from isn't a property of the data, and devices aren't a stable
  partition anyway — two real devices reported concurrently for ~2 months in
  the historical archive. Both are row fields. Two boxes capturing the same
  bus produce different rows (`device_id` differs), hence different content
  ids, hence separate objects; they only collapse into one if both are
  configured with the same `device_id`, which is a misconfiguration.

  Every flush is written to a local spool file first and uploaded from
  there, reachable S3 or not — there is no online/offline mode. The write is
  tmp → fsync → rename → fsync the directory, so a file under its final name
  is always complete and durable; a power cut loses only frames still in
  RAM. Spool filenames mirror the key
  (`disk.<yyyymmdd>_<HHMMSS>-<proto>-<content_id>.ndjson.gz`), so the upload
  builds the S3 key from the filename alone — nothing decompressed, no state
  kept. They sort chronologically, which the oldest-first eviction and
  upload order both rely on.

## `raw`

One JSON object per line (ndjson), gzip-compressed per batch. Six fields,
the same for every protocol:

| field | type | null? | notes |
|-------|------|-------|-------|
| `ts` | string (ISO 8601) | no | capture time, `CLOCK_REALTIME`, UTC |
| `mono` | number | yes | `CLOCK_MONOTONIC` at capture, seconds, µs resolution — see below. Null where the capture path had no monotonic clock, e.g. an import out of a database |
| `device_id` | string | no | which machine captured it. Hostname unless `--device-id` overrides it |
| `src` | string | yes | which input on that machine: `can0`, `/dev/ttyUSB0`, a peer address. Null where not meaningful |
| `proto` | string | no | what the bytes are — see the registry below |
| `raw` | string | no | the frame or sentence **verbatim**, encoded per that registry |

```json
{"ts":"2026-08-24T12:00:00.012345+00:00","mono":58757.02,"device_id":"boat-pi","src":"can0","proto":"n2k","raw":"09f80102#a54dca182530bb1d"}
{"ts":"2026-08-24T12:00:04.918233+00:00","mono":58761.92,"device_id":"boat-pi","src":"/dev/ttyUSB0","proto":"n0183","raw":"$GPRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*6A"}
```

### Protocol registry

Adding a protocol adds a row here. It does not add a field, change the
record, or change the key layout — that is the whole point of the envelope.

| `proto` | `raw` encoding | example |
|---|---|---|
| `n2k` | `<8 hex CAN id>#<payload hex>`, lowercase — the candump ASCII form. The identifier is part of the frame, so it is part of `raw`; payload length is implicit in the hex length, so no DLC is stored | `09f80102#a54dca182530bb1d` |
| `n0183` | the sentence verbatim, `$`/`!` lead-in and `*hh` checksum included | `$GPRMC,123519,A,4807.038,N*6A` |
| *a future text protocol* | verbatim | |
| *a future binary protocol* | lowercase hex, or base64 above ~1 KB | |

**`proto` may contain only lowercase alphanumerics and `.`** — no `-` and no
`_`, because those delimit the object name and the spool filename. A proto
that broke that parse would produce keys this tool cannot read back and,
with a PUT-only credential, cannot delete either. `record_line()` refuses
them rather than letting one reach the bucket.

### Deriving what isn't stored

`pgn`, `src_addr`, `priority` and an 0183 sentence type were columns on
every row once. Each is a pure function of `raw`, and storing them was a bad
trade twice over:

- **They bought nothing at read time.** Objects are gzipped ndjson — there is
  no predicate pushdown, so you decompress and parse every line regardless.
  Recovering `pgn` is four bit operations on a value already in hand.
- **They froze a decoder into permanent storage.** A PDU1/PDU2 edge case
  decoded wrongly would be written into every object ever, unfixable. Stored
  raw, a decoder bug is a fix and a re-derive.

Same reasoning that keeps `mono` raw rather than as the derived epoch.
`src/nmea2s3/decode.py` computes them on read and is the definition of what
`raw` means for each protocol:

```python
from nmea2s3 import decode

decode.n2k("09f80102#a54dca182530bb1d")
# N2KFrame(can_id=167248130, pgn=129025, src_addr=2, priority=2, payload=b'\xa5M...')

decode.n0183("$GPRMC,123519,A,4807.038,N*6A")
# NMEA0183Sentence(talker='GP', sentence_type='RMC', fields=[...], checksum_ok=True)
```

The removal cost ~17% of every object: measured on 20k realistic rows,
166 → 143 bytes raw and 21.2 → 17.6 bytes gzipped, while *adding* `src` and
`proto`.

### `mono` — the second clock

`ts` is `CLOCK_REALTIME`, which is only as correct as the system clock was at
capture. An RTC-less SBC boots with a wrong clock and learns the true time
only when NTP first syncs, so frames captured before that carry
plausible-looking but wrong timestamps. `mono` is `CLOCK_MONOTONIC`, which
cannot jump.

Storing both makes the clock basis recoverable: **`ts - mono` is the boot
epoch**, constant while the clock is stable and shifted by exactly the step
size when NTP corrects it. Rows agreeing on `ts - mono` were stamped by the
same clock; rows that disagree were not. It also separates a clock correction
from a genuinely quiet bus, which are indistinguishable in `ts` alone — and
lets true elapsed time be measured across a correction, as
`(ts₂ - mono₂) - (ts₁ - mono₁)`.

**Nothing is repaired, and nothing is judged.** Frames are written exactly as
captured. Deciding whether a timestamp is trustworthy is analysis's job, and
analysis has both clocks on every row plus the boot epoch in the startup
audit entry.

The residual risk is filing: the key and the spool filename both derive from
`ts`, so a batch captured on a wrong clock files itself under whatever day
that clock believed — and if the logger's key cannot delete, as the README
recommends, a badly filed object can only be shadowed, never removed.

That window is open, deliberately. `systemd/nmea2s3.service` carries
`After=time-sync.target` but no `Wants=`, and `After=` is ordering only — it
does nothing unless something else on the box pulls the target in, which
nothing here does. The unit that would, `systemd-time-wait-sync.service`,
blocks indefinitely on a first sync, so on a boat with no internet it does
not delay capture, it prevents it. A logger that never starts loses
everything; one that starts on a wrong clock misfiles its first few objects
and records enough to prove it. Capture does not wait, and does not judge.

A hardware RTC is what closes the window. What makes it survivable without
one is that nothing is discarded. `mono` on every frame and `clock_epoch` in
the start audit entry identify which clock basis a run was stamped against,
and where the archive carries NMEA 0183 RMC, the sentences hold an
independent UTC reference of their own — `nmea2s3-update-pg` measures the
capture clock against it and stores the difference as an ordinary field,
`rmc_clock_offset`, surfaced by `sql/metrics.sql` as `clock_offset`. A step
there is the system clock moving.

Nothing in this project drops, rejects or errors on a row because of any of
that. The readings are recorded and the judgement is left to whoever is
asking — which is the same rule the logger follows, applied one layer up.

`mono` is stored raw rather than as the derived `ts − mono` epoch. The
derived form compresses far better but freezes a rounding and refresh policy
into an archive kept forever, for ~1 GB/year at a busy bus's frame rate.
Readers that trust their clock may ignore `mono` entirely.

## `_log` (operational audit log)

Prefix `_log/...` — **outside `raw/`**, because it is not capture data. Same
day-partitioned layout, but NOT gzip'd and NOT content-addressed: each entry
is a distinct real event, so hashing content would wrongly collapse two
identical-looking-but-different actions into one, and entries are small
enough that being able to `cat` one matters more than the bytes. One plain
JSON object per action, key `_log/<yyyy>/<mm>/<dd>/<HHMMSS>-<random>.json`.

The logger writes two per run: a start entry, and a stop entry. A start with
no stop after it is a run that died partway — the case no exception handler
can report, because a SIGKILL or a pulled plug raises nothing to catch.

| field | type | notes |
|---------------|-------------------|-------|
| `timestamp` | string (ISO 8601) | when logged — the one place upload time IS the meaningful time |
| `application` | string | which tool wrote it. `nmea2s3-logger` here; other tools sharing a bucket write their own names. Read-only tools never appear: `nmea2s3-exporter` writes nothing. Nothing rewrites or removes an entry, so retired names persist — this logger's entries were filed under `flightrecorder_logger` before it was split into its own repo, and those stay in the archive |
| `host` | string | hostname of the machine that ran it |
| `exit_code` | integer | 0 = success; 1 = failed. Only tools that CHANGE something write entries, so a read-only run never appears |
| `comment` | string | human-readable summary |
| `details` | object | freeform, varies per application — see below for what this logger writes. A tool whose runs need explicit pairing may add its own `event: start`/`end` and a `run_id`; this logger does not, and its entries pair by `host` and time order |

Written with sorted keys and two-space indentation. A logger stop entry:

```json
{
  "application": "nmea2s3-logger",
  "comment": "logger stopped (signal) — rx=1284410 spooled=1284410 objects=68 dropped=0",
  "details": {
    "bad_clock": 0,
    "device_id": "boat-pi",
    "dropped": 0,
    "no_kernel_ts": 0,
    "objects": 68,
    "reason": "signal",
    "rx": 1284410,
    "spool_files": 0,
    "spooled": 1284410
  },
  "exit_code": 0,
  "host": "boat-pi",
  "timestamp": "2026-08-12T23:10:29.084194+00:00"
}
```

### What the logger puts in `details`

Start entry:

| key | meaning |
|---|---|
| `device_id`, `can_iface` | what was captured, and from where |
| `clock_epoch` | `time.time() - time.monotonic()` — the boot epoch, the same value `ts - mono` yields on every frame of this run |
| `mono_at_start` | `CLOCK_MONOTONIC` when capture began. On every platform that matters that clock starts at boot, so this value **is** the capture lost to booting — two real reboots cost 6.7 and 14.6 minutes |
| `spool_files` | left by the previous run, about to be replayed |

Stop entry:

| key | meaning |
|---|---|
| `rx`, `spooled`, `objects` | frames captured, frames written to the spool, spool files uploaded |
| `dropped` | frames lost to eviction — RAM and spool both count here, so this never under-reports |
| `no_kernel_ts` | frames the kernel gave no RX timestamp for, which fell back to `now()` |
| `bad_clock` | frames stamped before `TIMESTAMP_FLOOR` — recorded, never dropped |
| `reason` | `signal` for a clean SIGTERM (exit 0), `task_failed` for a loop falling over (exit 1). Both used to write exit 0 and identical text, so a gap in the data gave no clue which had happened |
| `spool_files` | still not uploaded. On a boat that is the only copy that exists |
