#!/usr/bin/env python3
"""
nmea2s3 — NMEA 2000 raw capture logger (SocketCAN -> S3).

Reads raw CAN frames from a SocketCAN interface (e.g. can0) and uploads them,
batched, to an S3-compatible bucket (DigitalOcean Spaces). Timestamps come
from the kernel (SO_TIMESTAMPNS), stamped in the driver's softirq rather than
on the asyncio wakeup, so they carry no scheduler latency. Each frame is
stored verbatim as `<8 hex CAN id>#<payload hex>` — the whole frame, nothing
decoded. See SCHEMA.md for the record shape, which is the same six fields
for every protocol in the archive, and decode.py for recovering `pgn`,
`src_addr` and `priority` on read.

Every timestamp in this codebase is UTC, strictly — never local time, never
naive. Linux's CLOCK_REALTIME (what SO_TIMESTAMPNS reads) is UTC-seconds-
since-epoch internally regardless of the SBC's configured local timezone, so
this isn't a conversion this code has to perform, only an invariant it must
not violate. record_line() enforces it for every writer: it raises rather
than serialize a timestamp that isn't tz-aware UTC, since every construction
path already produces one and a value that fails that check means a real
code bug, not a data condition to route around.

Timestamp assumptions beyond "which zone": SO_TIMESTAMPNS is CLOCK_REALTIME,
not CLOCK_MONOTONIC — accurate only insofar as the system clock was correct
at capture time. An RTC-less SBC has no correct clock until its first NTP
sync, so frames captured before that sync carry a wrong-but-plausible-looking
timestamp; TIMESTAMP_FLOOR below only catches the common case (a clock still
near its pre-sync default), counted as `bad_clock` in the stats line, never
dropped. Capture deliberately does NOT wait for NTP: the unit's
After=time-sync.target is ordering only and nothing here pulls that target
in, because the service which would blocks forever on a boat with no
internet — and a logger that never starts loses everything, where one
starting on a wrong clock misfiles a few objects and records enough to prove
it. Fit a hardware RTC module to remove the failure mode outright; short of
one, every frame carries the evidence instead. See SCHEMA.md for the full
writeup.

What TIMESTAMP_FLOOR cannot catch — a clock wrong by minutes or hours rather
than years — the second clock records. Every frame carries CLOCK_MONOTONIC
alongside its CLOCK_REALTIME `ts`, and monotonic cannot jump, so `ts - mono`
is the boot epoch: constant while the clock is stable, shifted by exactly
the step size when NTP corrects it. That pair is also the only way to tell a
clock correction apart from a genuinely quiet bus — identical in `ts` alone.

This file makes NO judgement about the clock. It records both readings and
writes every batch to `raw/`; deciding whether a timestamp is trustworthy
is analysis's job, and analysis has both clocks on every row plus the boot
epoch in the startup audit entry. An earlier version routed suspect batches
to a separate `n2k_quarantine/` prefix, which was deleted 2026-08-24 for
three reasons: it did not fix the thing it existed for (a quarantined
object is still filed under whatever day the bad clock believed — the
prefix only hides it from the default read path); the same finding is
recoverable on read for nothing, since `mono` is on every frame and the
boot epoch is in the start audit entry, so the verdict spent storage layout
restating what the rows already said; and answering "is the clock right"
needed `timedatectl`, the only subprocess and the only systemd coupling in
the capture path, whose semantics were subtle enough to produce two bugs in
two days. Keeping the logger dumb is worth more than a verdict that was
neither sufficient nor cheap.

S3 is the permanent store; the buffer/spool below exist only to survive
outages between here and there, not as long-term storage.

This is a cold path only: FLUSH_INTERVAL is tuned for archival object size
and count, not for freshness — there is currently no separate "hot path"
(e.g. a direct Postgres write) for live dashboards, so nothing downstream
sees data faster than FLUSH_INTERVAL. If a hot path is added later, it
should write independently of this loop rather than by shortening
FLUSH_INTERVAL back down for its sake.

Spool, then upload:
  Frames take exactly one route to S3, whether or not the network is up.
  Every flush writes the buffer to a local spool file (atomic: tmp ->
  fsync -> rename -> fsync the directory) in DISK_DIR, and a separate loop
  uploads spool files and deletes each one as it lands.

  Nothing here tracks whether S3 is reachable. There is no online/offline
  mode, no reachability probe, no recovery routine — an unreachable S3 just
  means a file is still sitting in the spool on the next pass. That is the
  entire outage story, and it is why this file has one write path instead
  of the three the previous online/disk/recovery split needed.

  Upload order is oldest-file-first, which is also ascending in ts. Nothing
  downstream should depend on that: keys are content-addressed and carry
  their own capture day, so an object arriving late or out of order is
  picked up by whatever next reprocesses the window covering it.

  - The spool directory must be a persistent mount (not tmpfs).
    Default: ~/n2k_fallback; --disk-dir wins, then NMEA2S3_DISK_DIR.
  - Stale .tmp files left by a prior crash are discarded on startup.
  - DISK_DIR is capped at MAX_DISK_BYTES. If an outage runs long enough to
    fill it, the OLDEST spool file is deleted to make room for the newest
    batch — S3 is the durable copy; the local spool is only a bridge and is
    not itself meant to be the last copy of old data.
  - If the spool write itself fails (full or failing disk), the batch is
    uploaded straight to S3 as a last resort, with the same key and the
    same bytes the spool file would have produced.

  This is a periodic spool, NOT a write-ahead log: frames live in RAM for
  up to FLUSH_INTERVAL before reaching disk. A power cut loses whatever is
  still in RAM.

  The RAM buffer is similarly capped at MAX_BUFFER_BYTES; once full, the
  OLDEST frames are dropped to make room for the newest (same policy as the
  disk spool). The byte accounting is the serialized ndjson line size, which
  is a payload budget rather than an RSS ceiling: Python objects cost a
  measured 2.3x on top, and MAX_BUFFER_BYTES is chosen so that 2.3x still
  fits inside the unit's MemoryMax. See the constant for the arithmetic.

Options:
  --can IFACE        — SocketCAN interface to read (default: can0)
  --log-level LEVEL  — logging level (default: INFO); overrides NMEA2S3_LOG_LEVEL
  --device-id ID     — device_id recorded in each row (default: hostname)

Environment variables (never hardcoded — see env.example):

Required:
  NMEA2S3_S3_ENDPOINT_URL    — the REGION endpoint, e.g.
                              https://syd1.digitaloceanspaces.com
  NMEA2S3_S3_BUCKET          — bucket name
  NMEA2S3_S3_ACCESS_KEY_ID, NMEA2S3_S3_SECRET_ACCESS_KEY — passed explicitly
                              to boto3, not the standard AWS_* names — see
                              make_s3_client() in ndjson.py

Optional:
  NMEA2S3_S3_REGION   — default: us-east-1 (DO Spaces ignores it, boto3 requires it)
  NMEA2S3_DISK_DIR    — spool directory (default: ~/n2k_fallback; --disk-dir wins)
  NMEA2S3_LOG_LEVEL   — logging level (default: INFO; overridden by --log-level)

Install: pipx install nmea2s3 — boto3 and the stdlib, nothing else. Deploy
with systemd/nmea2s3.service.
"""

import argparse
import asyncio
import gzip
import logging
import os
import signal
import socket
import struct
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple, Optional

from botocore.exceptions import BotoCoreError, ClientError

from . import __version__
from .audit_log import log_action_safely
from .ndjson import (
    group_by_day, gzip_and_id_stream, make_s3_client,
    put_object_gz, record_line, required_env, s3_key,
)

# The name this tool files its `_log/` entries under. Renamed from
# `flightrecorder_logger` when the logger was split out into its own
# repo; `_log/` is permanent (these credentials cannot delete), so the
# old name stays in the archive forever and a query spanning the cutover
# has to accept both.
APPLICATION = "nmea2s3-logger"

# ── Configuration ──────────────────────────────────────────────────────────
# Credentials come only from the environment, never hardcoded here, and are
# read when the logger is CONSTRUCTED rather than at import. Reading them at
# import made `nmea2s3 --help` exit with "Missing required environment
# variable" before argparse ever ran — invisible to a systemd unit that
# always has an EnvironmentFile, and the first thing anyone installing the
# command hits. Startup still fails fast and loudly on a real
# misconfiguration; it just fails at startup rather than at import.

class _S3Config(NamedTuple):
    endpoint_url:      str
    bucket:            str
    region:            str
    access_key_id:     str
    secret_access_key: str


def _s3_config() -> _S3Config:
    return _S3Config(
        required_env("NMEA2S3_S3_ENDPOINT_URL"),
        required_env("NMEA2S3_S3_BUCKET"),
        os.environ.get("NMEA2S3_S3_REGION", "us-east-1"),
        required_env("NMEA2S3_S3_ACCESS_KEY_ID"),
        required_env("NMEA2S3_S3_SECRET_ACCESS_KEY"),
    )


DEVICE_ID = socket.gethostname()

# S3 is a cold/archival path only — no hot path (direct-to-Postgres for
# live Grafana freshness) exists yet, so this interval is tuned purely for
# archival efficiency: 5 min gives ~500KB-1.5MB gzip'd objects at realistic
# bus loads (measured), a comfortable reprocessing chunk size, and a much
# lower object count over a year than a short interval would. If a hot path
# is added later, this can safely grow further since freshness would no
# longer depend on it. Note this also sets the RAM crash-loss window (see
# module docstring) — there is no independent write-ahead disk log, so a
# power cut still loses whatever hasn't been flushed yet.
#
# UPLOAD_INTERVAL is unrelated to any of that: it only decides how promptly
# an already-durable spool file reaches S3, so it is short and cheap. When
# the spool is empty a pass costs one directory listing and no requests.
FLUSH_INTERVAL  = 300.0  # seconds — buffer -> spool file
UPLOAD_INTERVAL = 30.0   # seconds — spool files -> S3
STATS_INTERVAL  = 60.0   # seconds between stats log lines

# RAM budget, counted in serialized ndjson bytes. Bounded by the unit's
# MemoryMax=128M, NOT by what the buffer would ideally hold — an OOM kill
# loses the whole buffer rather than just its oldest end, which is the one
# outcome this cap exists to prevent. Disk is the overflow.
#
# This is a backstop, not the outage mechanism — the SPOOL is that, and it
# holds days. The buffer only has to bridge to the next flush, and only
# matters at all when the spool write AND a direct upload have both failed.
#
# Sized against the unit's MemoryMax rather than against how long anyone
# wishes it would last. Measured end-to-end, peak RSS at the cap:
#
#   24 MB payload -> 126 MB resident, 12 min of frames at 207 msg/s
#   48 MB payload -> 191 MB resident, 25 min
#   64 MB payload -> 237 MB resident, 33 min
#
# 48MB against MemoryMax=512M leaves 2.7x headroom. Two things dominate and
# were both missed by an earlier version of this comment: the interpreter
# plus boto3 occupy ~65 MB before a single frame arrives, and Python objects
# cost a measured 2.3x the payload they hold (386 bytes for a 165-byte row).
#
# Getting this wrong is not a soft failure. If the cgroup limit binds before
# this one does, the kernel kills the process and the WHOLE buffer is lost,
# instead of this cap shedding its oldest end and carrying on.
MAX_BUFFER_BYTES = 48 * 1024 * 1024   # ~305k frames, ~25 min at 207 msg/s

# Spool budget. Eviction here is real, permanent data loss, so this is sized
# to make it a last resort rather than a routine outcome, against an 8GB
# eMMC. Raised from 500MB, which would have started destroying the oldest
# batches while ~90% of the disk sat empty — the standing rule is to fill
# the disk before discarding anything.
#
# ~6 days, NOT the ~20 recorded when the cap was raised. That figure assumed
# 150 msg/s and 8.1 gzip'd bytes/row, both synthetic; a real bus capture
# (27,687 rows over 134 s) measured 207 msg/s and 19.2 bytes/row, so
# 2 GB / (207 x 19.2) = 6.3 days — from one two-minute sample, so treat it
# as an order of magnitude rather than a figure.
MAX_DISK_BYTES   = 2 * 1024 * 1024 * 1024  # 2GB spool budget, oldest-dropped when full

# Must be a persistent mount — not tmpfs. Use a USB stick if SD wear is a concern.
DISK_DIR = Path(os.environ.get("NMEA2S3_DISK_DIR", Path.home() / "n2k_fallback"))

# ── Logging ──────────────────────────────────────────────────────────────

log = logging.getLogger("n2k")

# ── Data model ─────────────────────────────────────────────────────────────

# What this logger writes into every row's `proto`, and into the middle
# segment of every key it produces.
PROTO = "n2k"


class Frame(NamedTuple):
    """One captured CAN frame, already serialized.

    The ndjson line is built once, at capture, and carried from here on —
    it is what lands on disk and in S3, what content_id() hashes, and what
    both byte caps are accounted in. An earlier version kept the fields
    separately and re-serialized on demand, which meant the same frame was
    turned into JSON two to four times (once for the byte accounting at
    append, again per eviction, again at flush) and left two serializations
    that had to agree for the accounting to be correct. It also cost 3.5x
    the frame's own payload size in RAM against a 128M MemoryMax; this form
    measures 2.3x.
    """
    ts:     datetime   # CLOCK_REALTIME at capture — kernel softirq timestamp
    # CLOCK_MONOTONIC at capture, recorded raw alongside `ts`. Monotonic
    # cannot jump, so the pair pins down which clock basis `ts` was stamped
    # against: `ts - mono` is the boot epoch, constant while the clock is
    # stable and shifted by exactly the step size when NTP corrects it.
    #
    # Stored raw rather than as the derived (constant, cheaper) epoch on
    # purpose. The derived form compresses ~19x better, but that is ~1 GB
    # a year against a 250 GB allowance — not worth freezing a rounding
    # and refresh policy into an archive that is kept forever. Raw keeps
    # every downstream interpretation open; the logger stays dumb.
    #
    # Read in userspace, so it trails the kernel's softirq `ts` by the
    # delivery latency (sub-millisecond). Irrelevant for identifying a
    # clock basis, and `ts` remains the timestamp to use for anything
    # needing real precision.
    mono:   float
    line:   str        # the ndjson row, newline-terminated
    nbytes: int        # len(line) in utf-8 — the unit both caps count in


def build_frame(ts: datetime, mono: float, can_id: int, payload: bytes,
                 src: str = "can0") -> Frame:
    """Serialize one captured frame. Decodes nothing.

    `raw` is the candump ASCII form, `<8 hex CAN id>#<payload hex>`,
    lowercase — the complete frame as it came off the wire, identifier
    included. An earlier shape stored the identifier as an integer column
    beside a hex `data` column, plus `pgn`/`src_addr`/`priority` decoded
    from it. Those three are a pure function of the identifier, so they
    were ~17% of every object spent restating what was already there, and
    they baked one particular decoder into an archive that is never
    rewritten. decode.n2k() computes them on read instead.

    The UTC check lives in record_line(), which every writer shares.
    """
    line = record_line(ts, mono, DEVICE_ID, src, PROTO,
                        f"{can_id:08x}#{payload.hex()}")
    return Frame(ts, mono, line, len(line.encode("utf-8")))


# ── S3 helpers ─────────────────────────────────────────────────────────────
# record_line, gzip_and_id_stream, s3_key, group_by_day, make_s3_client and
# put_object_gz all live in ndjson.py — shared with the exporter and with any
# importer writing into the same archive, so neither the record shape nor the
# key format can drift between two writers.

class _Object(NamedTuple):
    """One gzipped ndjson object: the unit that becomes both a spool file
    and, later, an S3 key. Both names are derived from these same fields,
    which is what lets a spool file be uploaded straight from its filename
    with nothing decompressed and no state kept about how it got there."""
    day:         str    # YYYYMMDD, the batch's own capture day
    time_of_day: str    # HHMMSS of the first row
    cid:         str    # content id — hash of the pre-gzip text
    rows:        int
    body:        bytes  # gzipped ndjson


def _objects(batch: list[Frame]) -> list[_Object]:
    """Split a batch into the objects it becomes, one per capture day.

    The single place a batch is turned into named bytes. Both sinks — the
    spool file and a direct S3 upload — consume this, so a batch cannot end
    up under two different names depending on which path it took.
    """
    out = []
    for day, group in group_by_day(batch):
        # Streamed rather than joined: building one string of the whole group
        # and compressing that kept an extra full copy of the batch alive at
        # exactly the moment RSS peaks. Measured at the buffer cap, the flush
        # transient roughly doubled the buffer's footprint. Same function the
        # migration tool uses, so the two writers cannot produce different
        # bytes for the same rows.
        cid, body = gzip_and_id_stream(f.line for f in group)
        out.append(_Object(day, group[0].ts.strftime("%H%M%S"), cid, len(group), body))
    return out


def _spool_name(obj: _Object) -> str:
    """disk.<yyyymmdd>_<HHMMSS>-<proto>-<content_id>.ndjson.gz

    Sorts chronologically, which is what the spool's oldest-first eviction
    and upload order both rely on.
    """
    return f"disk.{obj.day}_{obj.time_of_day}-{PROTO}-{obj.cid}.ndjson.gz"


def _key_for_spool_file(name: str) -> str:
    """Inverse of _spool_name: the S3 key a spool file belongs at.

    Every piece of the key is read straight from the filename, so an upload
    needs neither the batch it came from nor a decompression pass.
    """
    stem = name.removeprefix("disk.").removesuffix(".ndjson.gz")
    day, rest = stem.split("_", 1)
    time_of_day, proto, cid = rest.split("-", 2)
    return s3_key(proto, day, time_of_day, cid)


# ── Logger ─────────────────────────────────────────────────────────────────

# First retry is fast, because the common case (interface bounced, cable
# reseated) recovers immediately. Repeated failures back off to
# CAN_RECONNECT_MAX: a missing interface — wrong overlay, nothing plugged in
# at boot — is a permanent condition until someone fixes it, and retrying at
# 1s forever wrote ~86,000 error lines a day into the journal.
CAN_RECONNECT_DELAY = 1.0   # seconds — first reopen attempt
CAN_RECONNECT_MAX   = 30.0  # seconds — ceiling after repeated failures

# struct can_frame (Linux): u32 can_id, u8 dlc, 3 pad bytes, 8 data bytes = 16 bytes
_CAN_FRAME_FMT  = "=IB3x8s"
_CAN_FRAME_SIZE = struct.calcsize(_CAN_FRAME_FMT)
CAN_RTR_FLAG = 0x40000000  # remote-transmission request
CAN_ERR_FLAG = 0x20000000  # error frame
CAN_EFF_MASK = 0x1FFFFFFF  # 29-bit identifier mask

# Linux asm-generic/socket.h — not always exposed by Python's socket module.
SO_TIMESTAMPNS  = getattr(socket, "SO_TIMESTAMPNS",  35)
SCM_TIMESTAMPNS = getattr(socket, "SCM_TIMESTAMPNS", SO_TIMESTAMPNS)
_CMSG_BUFSIZE   = 128       # ample for one struct timespec
CAN_RCVBUF      = 1 << 20   # 1 MiB socket receive queue

# SO_TIMESTAMPNS is CLOCK_REALTIME, not CLOCK_MONOTONIC — it is only as
# correct as the system clock was at the moment the frame arrived. An
# RTC-less SBC (e.g. a bare Raspberry Pi) starts every boot with an
# arbitrary clock until NTP completes its first sync, so early frames can
# carry a timestamp that is wrong but not obviously so. This floor only
# catches the common failure mode — a clock still stuck near its pre-NTP
# default — not a clock that is merely off by minutes/hours.
TIMESTAMP_FLOOR = datetime(2020, 1, 1, tzinfo=timezone.utc)



class N2KLogger:
    # disk_dir is passed rather than read from the environment so a caller can
    # win outright. Under systemd that caller is ExecStart=: EnvironmentFile=
    # always overrides Environment=, so a unit cannot pin the spool with an
    # environment variable — the env file gets the last word and would send
    # the spool somewhere the sandbox refuses to let it write. The command
    # line is not in that contest.
    def __init__(self, can_iface: str = "can0", disk_dir: str | Path | None = None):
        self.can_iface  = can_iface   # SocketCAN interface name
        self.disk_dir   = Path(disk_dir) if disk_dir else DISK_DIR
        self.buffer: deque[Frame] = deque()
        self.buffer_bytes = 0
        self.running    = False
        self._s3_down   = False   # for log hygiene only, never for routing
        cfg = _s3_config()
        self.bucket = cfg.bucket
        self.s3 = make_s3_client(cfg.endpoint_url, cfg.region,
                                 cfg.access_key_id, cfg.secret_access_key)
        self.rx       = 0
        self.spooled  = 0   # frames written to the spool
        self.objects  = 0   # spool files uploaded to S3
        self.dropped  = 0
        self.no_kernel_ts = 0   # frames the kernel gave no RX timestamp for
        self.bad_clock = 0      # frames timestamped before TIMESTAMP_FLOOR

    async def start(self):
        log.info(f"[n2k] logger starting — device={DEVICE_ID} can={self.can_iface} "
                  f"bucket={self.bucket}")
        self.disk_dir.mkdir(parents=True, exist_ok=True)
        for stale in self.disk_dir.glob("*.tmp"):
            stale.unlink(missing_ok=True)
        self.running = True
        await asyncio.to_thread(
            log_action_safely, self.s3, self.bucket, APPLICATION, 0,
            f"logger started — device={DEVICE_ID} can={self.can_iface}",
            {
                "device_id": DEVICE_ID, "can_iface": self.can_iface,
                # Session origin, so a reader can tell which boot a run
                # belongs to without inferring it from frame data.
                "clock_epoch": round(time.time() - time.monotonic(), 6),
                # The SAME clock every frame's `mono` is read from, so this
                # is directly comparable with them: it is `mono` at the moment
                # capture began. On every platform that matters that clock
                # starts at boot, so a small value means a fresh boot and the
                # value itself IS the capture lost to booting — two real
                # reboots cost 6.7 and 14.6 minutes, and establishing that
                # meant subtracting clock_epoch from the first frame by hand.
                #
                # Deliberately not /proc/uptime: this is stdlib, needs no
                # filesystem, and cannot disagree with the frames.
                "mono_at_start": round(time.monotonic(), 1),
                # Anything here was left by the previous run and is about to
                # be replayed. A count, not a size: the size needed .stat() on
                # every file, which meant a second walk and a race with the
                # upload loop deleting them — machinery for a number the
                # journal already prints.
                "spool_files": len(list(self.disk_dir.glob("disk.*.ndjson.gz"))),
            },
        )

        tasks = [
            asyncio.create_task(self._can_listener()),
            asyncio.create_task(self._flush_loop()),
            asyncio.create_task(self._upload_loop()),
            asyncio.create_task(self._stats_loop()),
        ]
        # None of these should ever return on its own, so if one does,
        # stop the others and re-raise whatever stopped it: the process
        # exits non-zero and systemd restarts it.
        #
        # A bare gather() here was a silent-failure hole. gather propagates
        # the first exception but does NOT cancel its siblings, so a crashed
        # _flush_loop left _can_listener running and filling the buffer, main()
        # still waiting on its shutdown event, and the process alive and
        # apparently healthy while never uploading another byte.
        try:
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        for t in done:
            t.result()      # re-raises if that is why it stopped
        raise RuntimeError("[n2k] a logger loop exited on its own")

    async def stop(self, reason: str = "unknown"):
        log.info("[n2k] stopping — flushing buffer to spool...")
        self.running = False
        # Spool first, then one best-effort upload pass. Anything still in
        # the spool after this is replayed by the next start; the flush is
        # the part that must not be skipped, since only it is holding data
        # that exists nowhere else.
        await self._flush_buffer()
        await self._upload_spool()
        log.info("[n2k] stopped")
        await asyncio.to_thread(
            log_action_safely, self.s3, self.bucket, APPLICATION,
            0 if reason == "signal" else 1,
            f"logger stopped ({reason}) — rx={self.rx} spooled={self.spooled} "
            f"objects={self.objects} dropped={self.dropped}",
            {
                "device_id": DEVICE_ID, "rx": self.rx, "spooled": self.spooled,
                "objects": self.objects,
                "dropped": self.dropped, "no_kernel_ts": self.no_kernel_ts,
                "bad_clock": self.bad_clock,
                # A clean SIGTERM from a reboot and a task falling over both
                # wrote exit_code 0 and identical text, so a gap in the data
                # gave no clue which had happened.
                "reason": reason,
                # Data that exists nowhere else yet. Non-zero here means the
                # next start has to replay it, or it is lost with the card.
                "spool_files": len(list(self.disk_dir.glob("disk.*.ndjson.gz"))),
            },
        )

    # ── CAN listener (SocketCAN) ────────────────────────────────────────────

    @staticmethod
    def _cmsg_timestamp(ancdata) -> Optional[datetime]:
        """Extract the kernel RX timestamp from recvmsg ancillary data.

        SCM_TIMESTAMPNS carries a struct timespec stamped in the driver's softirq,
        before the frame reaches userspace — so it is free of scheduler latency,
        which is exactly the error that grows with bus load.
        Returns None if the kernel did not supply one.
        """
        for level, ctype, cdata in ancdata:
            if level != socket.SOL_SOCKET or ctype != SCM_TIMESTAMPNS:
                continue
            # struct timespec is 2 longs: 16 bytes on 64-bit, 8 on 32-bit.
            fmt = "qq" if len(cdata) >= 16 else "ll"
            sec, nsec = struct.unpack(fmt, cdata[:struct.calcsize(fmt)])
            # Built from integers, not sec + nsec/1e9: at epoch-scale magnitudes
            # a float carries only ~0.5 µs of resolution, which would quantise
            # the very timestamps this function exists to keep accurate.
            return (datetime.fromtimestamp(sec, timezone.utc)
                    + timedelta(microseconds=nsec // 1000))
        return None

    def _append(self, frame: Frame):
        self.buffer.append(frame)
        self.buffer_bytes += frame.nbytes
        self._trim()

    def _trim(self):
        """Drop the oldest frames until the buffer is back under
        MAX_BUFFER_BYTES. S3 is the durable copy and the buffer only bridges
        the gap to the next flush, so under sustained overload losing the
        oldest not-yet-written frames beats losing the newest ones."""
        while self.buffer and self.buffer_bytes > MAX_BUFFER_BYTES:
            evicted = self.buffer.popleft()
            self.buffer_bytes -= evicted.nbytes
            self.dropped += 1

    def _take_buffer(self) -> list[Frame]:
        """Remove the entire buffer and hand it to the caller.

        Taking the batch OUT is what makes a flush safe: while it is out,
        nothing else can evict or reorder the frames being written. The
        previous form snapshotted the buffer and afterwards dropped the N
        oldest by position — but _append's eviction runs during the await,
        so once it had evicted k frames, that drop discarded k newly
        arrived frames which had never been written anywhere.
        """
        batch = list(self.buffer)
        self.buffer.clear()
        self.buffer_bytes = 0
        return batch

    def _return_buffer(self, batch: list[Frame]):
        """Put an unwritten batch back at the front, oldest first. Frames
        that arrived during the failed write are newer, so they stay behind
        it and the buffer remains in ts order."""
        self.buffer.extendleft(reversed(batch))
        self.buffer_bytes += sum(f.nbytes for f in batch)
        self._trim()

    def _drain(self, sock: socket.socket, dead: asyncio.Event):
        """Read every frame the socket has ready. Called by the event loop when
        the CAN fd becomes readable — no Task or TimerHandle per frame."""
        while True:
            try:
                frame, ancdata, _flags, _addr = sock.recvmsg(_CAN_FRAME_SIZE, _CMSG_BUFSIZE)
            except (BlockingIOError, InterruptedError):
                return                      # socket drained
            except OSError as e:
                if self.running:
                    log.error(f"[n2k] CAN read error on {self.can_iface}: {e}")
                dead.set()
                return

            if len(frame) < _CAN_FRAME_SIZE:
                continue
            can_id, dlc, payload = struct.unpack(_CAN_FRAME_FMT, frame)
            # Skip error/remote frames; keep only the 29-bit identifier.
            if can_id & (CAN_ERR_FLAG | CAN_RTR_FLAG):
                continue
            can_id &= CAN_EFF_MASK

            ts = self._cmsg_timestamp(ancdata)
            if ts is None:
                ts = datetime.now(timezone.utc)
                self.no_kernel_ts += 1
            elif ts < TIMESTAMP_FLOOR:
                # Recorded as-is, never dropped — S3 is the durable copy and
                # a wrong-but-present timestamp is still useful evidence that
                # the clock was unsynced. _stats_loop surfaces the count so
                # it doesn't go unnoticed.
                self.bad_clock += 1

            self._append(build_frame(ts, time.monotonic(), can_id, payload[:dlc]))
            self.rx += 1

    async def _can_listener(self):
        iface = self.can_iface
        loop  = asyncio.get_running_loop()
        delay = CAN_RECONNECT_DELAY
        while self.running:
            try:
                sock = socket.socket(socket.AF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
                sock.bind((iface,))
                sock.setblocking(False)
                # Kernel RX timestamps, stamped at softirq rather than on wakeup.
                sock.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
                # A deep receive queue absorbs GC pauses and flush stalls without
                # the kernel having to discard frames.
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, CAN_RCVBUF)
            except Exception as e:
                if self.running:
                    log.error(f"[n2k] CAN open failed on {iface}: {e} — retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, CAN_RECONNECT_MAX)
                continue

            delay = CAN_RECONNECT_DELAY
            log.info(f"[n2k] CAN listening on {iface}")
            dead = asyncio.Event()
            loop.add_reader(sock.fileno(), self._drain, sock, dead)
            try:
                await dead.wait()           # cancelled cleanly on shutdown
            finally:
                loop.remove_reader(sock.fileno())
                sock.close()

            if self.running:
                log.info(f"[n2k] CAN reopening {iface} in {delay:.0f}s...")
                await asyncio.sleep(delay)

    # ── Flush loop: buffer -> spool ──────────────────────────────────────────

    async def _flush_loop(self):
        """The only thing that empties the buffer, and the only thing that
        writes a spool file. It does not know or care whether S3 is up."""
        while self.running:
            await asyncio.sleep(FLUSH_INTERVAL)
            await self._flush_buffer()

    async def _flush_buffer(self):
        """Write the buffer to the spool, or straight to S3 if the spool
        write fails.

        The batch leaves the buffer up front and goes back only if both
        writes failed, so a failed write never loses frames and a concurrent
        eviction can never touch frames that are mid-write.
        """
        batch = self._take_buffer()
        if not batch:
            return
        # Off the event loop: gzipping a full batch is a few hundred ms of
        # CPU, and the CAN reader is a callback on this same loop.
        objects = await asyncio.to_thread(_objects, batch)
        if await asyncio.to_thread(self._write_spool, objects):
            self.spooled += len(batch)
            return

        # Spool write failed — a full or failing disk, or DISK_DIR gone
        # read-only. Try S3 directly rather than sitting on the data while
        # a perfectly good network is available. Same keys, same bytes the
        # spool file would have produced.
        log.warning("[n2k] spool write failed — trying S3 directly")
        if await self._upload_objects(objects):
            return
        self._return_buffer(batch)

    # ── Upload loop: spool -> S3 ─────────────────────────────────────────────

    async def _upload_loop(self):
        """Upload spool files, forever, whether or not anything is there.

        Runs on its own clock and holds no state about S3's health: a failed
        upload is not a mode to enter, just a file still present next pass.
        Uploads immediately on startup, which is also what replays whatever
        a previous run left behind.
        """
        while self.running:
            await self._upload_spool()
            await asyncio.sleep(UPLOAD_INTERVAL)

    async def _upload_spool(self):
        """Upload every spool file, oldest first, deleting each as it lands.

        Stops at the first failure and leaves the rest for the next pass,
        so a dead network costs one failed request per pass rather than one
        per file.
        """
        for fp in sorted(self.disk_dir.glob("disk.*.ndjson.gz")):
            try:
                body = await asyncio.to_thread(fp.read_bytes)
            except OSError as e:
                log.error(f"[n2k] cannot read spool file {fp.name}: {e} — skipping")
                continue

            key = _key_for_spool_file(fp.name)
            try:
                await asyncio.to_thread(put_object_gz, self.s3, self.bucket, key, body)
            except (BotoCoreError, ClientError) as e:
                # Expected whenever the boat is out of coverage. Logged once
                # per outage, not once every UPLOAD_INTERVAL.
                if not self._s3_down:
                    self._s3_down = True
                    log.warning(f"[n2k] S3 unreachable ({e}) — spooling to {self.disk_dir}")
                return

            if self._s3_down:
                self._s3_down = False
                log.info("[n2k] S3 reachable again — spool draining")
            self.objects += 1
            try:
                fp.unlink(missing_ok=True)
            except OSError as e:
                # The object is safely in S3; a spool file we cannot delete
                # would be re-uploaded next pass to the same content-derived
                # key, which is a harmless overwrite, not a duplicate.
                log.warning(f"[n2k] could not delete uploaded spool file {fp.name}: {e}")
            log.info(f"[n2k] uploaded {fp.name} -> s3://{self.bucket}/{key}")

    async def _upload_objects(self, objects: list["_Object"]) -> bool:
        """Upload a batch's objects directly, bypassing the spool. Only used
        when the spool write failed."""
        try:
            for obj in objects:
                key = s3_key(PROTO, obj.day, obj.time_of_day, obj.cid)
                await asyncio.to_thread(put_object_gz, self.s3, self.bucket, key, obj.body)
                self.objects += 1
            return True
        except (BotoCoreError, ClientError) as e:
            log.error(f"[n2k] direct S3 upload failed too: {e}")
            return False

    # ── Spool write (atomic: tmp -> fsync -> rename -> fsync dir) ────────────

    def _disk_usage(self) -> int:
        return sum(f.stat().st_size for f in self.disk_dir.glob("disk.*.ndjson.gz"))

    @staticmethod
    def _count_gz_rows(path: Path) -> int:
        """Row count of a gzip'd ndjson spool file, for loss accounting on
        eviction. Best-effort: an unreadable/corrupt file counts as 0 rather
        than raising, since the eviction itself must still proceed."""
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                return sum(1 for _ in f)
        except Exception:
            return 0

    def _evict_spool_for(self, incoming_size: int):
        """Delete the OLDEST spool files until there's room for incoming_size,
        or nothing left to delete. S3 is durable; the spool is only a bridge,
        so under a sustained outage past MAX_DISK_BYTES the oldest not-yet-
        uploaded batch is what gets sacrificed.

        Each evicted file is real, permanent data loss — counted into
        self.dropped (same counter the RAM buffer's own eviction uses) so
        the stats line never under-reports how much was actually lost."""
        files = sorted(self.disk_dir.glob("disk.*.ndjson.gz"))  # sorts chronologically
        usage = self._disk_usage()
        while files and usage + incoming_size > MAX_DISK_BYTES:
            victim = files.pop(0)
            try:
                freed = victim.stat().st_size
                lost_rows = self._count_gz_rows(victim)
                victim.unlink(missing_ok=True)
                usage -= freed
                self.dropped += lost_rows
                log.warning(f"[n2k] spool full — dropped oldest batch {victim.name} "
                            f"({freed} bytes, {lost_rows} rows)")
            except Exception as e:
                log.error(f"[n2k] failed to evict {victim.name}: {e}")
                break

    def _fsync_dir(self):
        """Make the renames above durable, not just visible.

        This is the fourth step of the atomic-write idiom and the easiest
        one to leave out: fsync'ing the file puts its CONTENTS on stable
        storage, but the directory entry that names it is a separate piece
        of metadata, and until the filesystem commits it the rename can be
        lost. On ext4 with defaults that window is the journal commit
        interval — 5 seconds. A power cut inside it leaves the batch as a
        .tmp that startup discards, so the flush would have reported
        success for data that no longer exists.

        Costs one fsync per flush (not per file), i.e. one every five
        minutes.
        """
        fd = os.open(self.disk_dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _write_spool(self, objects: list["_Object"]) -> bool:
        """Write each object to its own spool file, atomically.

        A False return means the batch has not been fully placed and is
        still the caller's problem. Objects written before the failure stay
        on disk; they carry content-derived names, so a later retry that
        re-uploads them resolves to the same key with the same bytes — a
        harmless overwrite, never a duplicate.

        A filename already on disk is a no-op — same content already spooled,
        since the name is content-derived.
        """
        tmp = None
        renamed = False
        try:
            for obj in objects:
                path = self.disk_dir / _spool_name(obj)
                if path.exists():
                    log.debug(f"[n2k] spool: {path.name} already written, skipping")
                    continue
                if len(obj.body) > MAX_DISK_BYTES:
                    # A single batch bigger than the whole spool budget. Written
                    # in full regardless (no truncation implemented) — eviction
                    # below will clear every other spool file trying to make
                    # room, and disk usage will still exceed MAX_DISK_BYTES
                    # afterward. Only reachable if MAX_BUFFER_BYTES is ever
                    # configured close to or above MAX_DISK_BYTES.
                    log.warning(f"[n2k] batch ({len(obj.body)} bytes) exceeds MAX_DISK_BYTES on its "
                                f"own — writing it whole, spool budget will be exceeded")
                self._evict_spool_for(len(obj.body))
                tmp = path.with_suffix(".tmp")
                with open(tmp, "wb") as f:
                    f.write(obj.body)
                    f.flush()
                    os.fsync(f.fileno())
                tmp.rename(path)
                renamed = True
                log.info(f"[n2k] spooled {obj.rows} rows -> {path.name} ({len(obj.body)} bytes)")
            if renamed:
                self._fsync_dir()
            return True
        except Exception as e:
            log.error(f"[n2k] spool write failed: {e}")
            if tmp is not None:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
            return False

    # ── Stats ────────────────────────────────────────────────────────────────

    async def _stats_loop(self):
        while self.running:
            await asyncio.sleep(STATS_INTERVAL)
            pending = len(list(self.disk_dir.glob("disk.*.ndjson.gz")))
            log.info(
                f"[n2k] rx={self.rx} spooled={self.spooled} objects={self.objects} "
                f"dropped={self.dropped} no_kernel_ts={self.no_kernel_ts} "
                f"bad_clock={self.bad_clock} "
                f"buf={len(self.buffer)} ({self.buffer_bytes} bytes) "
                f"spool={pending} file(s) "
                # The boot epoch, the same `ts - mono` every row carries.
                # Constant while the clock is stable and stepping by exactly
                # the correction when NTP moves it, so a clock event is
                # visible in the journal without anything here judging it.
                f"epoch={time.time() - time.monotonic():.3f}"
            )


# ── Entry point ──────────────────────────────────────────────────────────

async def _run(args):
    n2k_logger = N2KLogger(can_iface=args.can, disk_dir=args.disk_dir)
    shutdown_event = asyncio.Event()
    loop           = asyncio.get_running_loop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    n2k_task  = asyncio.create_task(n2k_logger.start())
    stop_task = asyncio.create_task(shutdown_event.wait())

    # Whichever comes first: a shutdown signal, or the logger stopping by
    # itself. Waiting on the signal alone meant a logger that had already
    # died sat here forever instead of exiting for systemd to restart.
    done, _pending = await asyncio.wait(
        {n2k_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)

    if n2k_task in done:
        log.error("[n2k] logger stopped unexpectedly — flushing and exiting")
    else:
        log.info("Shutdown signal received")

    stop_task.cancel()
    n2k_task.cancel()
    await asyncio.gather(n2k_task, stop_task, return_exceptions=True)

    # Runs on both paths: a crash still gets the buffer written out.
    await n2k_logger.stop("task_failed" if n2k_task in done else "signal")

    if n2k_task in done:
        raise RuntimeError("logger stopped unexpectedly") from n2k_task.exception()


_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def main():
    """Console-script entry point — `nmea2s3`.

    Nothing above this reads the environment, so --help and --version work
    on a box that has never been configured.
    """
    # A module global, deliberately: build_frame() stamps every row with it,
    # and it is fixed for the life of the process. Without this declaration
    # the assignment below would bind a local and the override would
    # silently do nothing.
    global DEVICE_ID

    parser = argparse.ArgumentParser(
        prog="nmea2s3-logger",
        description="NMEA 2000 raw capture logger (SocketCAN -> S3)")
    parser.add_argument(
        "--can",
        metavar="IFACE",
        default="can0",
        help="SocketCAN interface to read (default: can0)",
    )
    parser.add_argument(
        "--log-level",
        metavar="LEVEL",
        choices=_LEVELS,
        default=None,
        help=f"logging level ({', '.join(_LEVELS)}); default: $NMEA2S3_LOG_LEVEL or INFO",
    )
    parser.add_argument(
        "--disk-dir",
        metavar="DIR",
        default=None,
        help="spool directory; wins over $NMEA2S3_DISK_DIR (default: ~/n2k_fallback)",
    )
    parser.add_argument(
        "--device-id",
        metavar="ID",
        default=None,
        help="device_id recorded in each row (default: hostname) — e.g. 'test' for a manual test run",
    )
    parser.add_argument("--version", action="version", version=f"nmea2s3-logger {__version__}")
    args = parser.parse_args()

    if args.device_id:
        DEVICE_ID = args.device_id

    _level_str = args.log_level or os.environ.get("NMEA2S3_LOG_LEVEL", "INFO").upper()
    _level = getattr(logging, _level_str, logging.INFO)
    logging.basicConfig(
        level=_level,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
