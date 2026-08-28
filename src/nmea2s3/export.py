#!/usr/bin/env python3
"""nmea2s3-exporter — read the archive back out as ndjson, CSV or candump.

Exports the captured archive (--source raw, the default) or the operational
audit log (--source _log); --format picks between ndjson (default), CSV and
candump — the ASCII form `candump -L` writes and canboat reads, so an
archived day decodes in one pipe:

    nmea2s3-exporter --proto n2k --format candump | canboat convert

without anything here learning what a PGN means.

Reads the day-partitioned key layout SCHEMA.md defines —
raw/<yyyy>/<mm>/<dd>/<HHMMSS>-<proto>-<cid>.ndjson.gz — and writes one
record per row. Every protocol shares one six-field record, so an export
can span n2k and n0183 in a single well-formed file; --proto narrows it to
one. That filter reads the object NAME, which a LIST already returned, so a
narrowed export downloads nothing it will discard — worth having, since the
protocols differ in volume by orders of magnitude.

_log is the operational audit log from audit_log.py — same day-partitioned
layout, but outside raw/ and each object is one plain (non-gzipped) JSON
record rather than gzip'd ndjson, so it is listed and read differently (see
iter_log_keys and READERS below). Its `details` field is a nested object: CSV has no native
way to hold that, so csv output flattens it to a JSON string per cell;
ndjson output keeps it nested, since ndjson supports that natively — one
real reason to prefer --format csv for _log specifically if you want a
flat file, since ndjson is otherwise the default everywhere.

Every format shares the entire pipeline (listing, date filtering,
retry/skip) — only the final per-row write differs (see make_writer) — so
this is one command with a --format switch rather than three that would
drift apart.

Defaults to stdout, ndjson, so `nmea2s3-exporter | less` or `| jq .` just
works with no flags at all; pass --since/--until to narrow the date range
and --proto to narrow to one protocol. Pass -o/--output PATH to write a file instead, or
bare -o with no PATH to write to an auto-named file in the current
directory: <timestamp>-<source>.<format>.

Date and protocol filtering are both cheap client-side checks against the
key itself — the day from its path, the protocol from its name — decided
before anything is fetched or decompressed. Object counts are
day-granularity (hundreds to low thousands), never per-row.

Streams row by row rather than buffering — an archive of hundreds of
millions of rows exports in constant memory.

Required environment variables (never hardcoded — see env.example):
  NMEA2S3_S3_ENDPOINT_URL, NMEA2S3_S3_BUCKET,
  NMEA2S3_S3_ACCESS_KEY_ID, NMEA2S3_S3_SECRET_ACCESS_KEY

Optional: NMEA2S3_S3_REGION (default us-east-1; DO Spaces ignores it, boto3
requires it)
"""

import argparse
import csv
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

from . import __version__
from .ndjson import (iter_keys, iter_log_keys, iter_rows_ndjson_gz,
                      make_s3_client, required_env)
from .retry import with_retries

# Column/field order matches SCHEMA.md and audit_log.py. Checked against
# both in tests/test_formats.py: naming the columns here means a new field
# has to be added in this dict too, or exports silently lose it.
#
# `raw` is ONE column set for every protocol — that is the whole point of the
# unified record. Before it, n2k and n0183 had different columns and could
# not be exported into a single file at all; now `--proto` narrows a stream
# that is otherwise uniform, and a CSV of mixed protocols is well-formed.
FIELDS = {
    "raw":  ["ts", "mono", "device_id", "src", "proto", "raw"],
    "_log": ["timestamp", "application", "host", "exit_code", "comment", "details"],
}


def load_config() -> dict:
    return {
        "s3_endpoint_url": required_env("NMEA2S3_S3_ENDPOINT_URL"),
        "s3_bucket": required_env("NMEA2S3_S3_BUCKET"),
        "s3_region": os.environ.get("NMEA2S3_S3_REGION", "us-east-1"),
        "s3_access_key_id": required_env("NMEA2S3_S3_ACCESS_KEY_ID"),
        "s3_secret_access_key": required_env("NMEA2S3_S3_SECRET_ACCESS_KEY"),
    }


def parse_date(s: str) -> date:
    return datetime.fromisoformat(s).date()


def iter_rows_log_json(s3, bucket: str, key: str):
    """_log: one plain (non-gzipped) JSON object per key — see
    audit_log.py. Yielded as-is, `details` still nested — format-
    specific flattening (or not) happens in make_writer, not here, so
    both output formats read from the same unmodified record."""
    def _get():
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    body = with_retries(_get, what=f"read {key}")
    yield json.loads(body.decode("utf-8"))


READERS = {
    "raw":  iter_rows_ndjson_gz,
    "_log": iter_rows_log_json,
}


# The interface `raw` was captured from, for a row that does not name one.
# Rows written by this logger always do; rows imported from a database that
# predates it do not, and the grammar has no way to say "unknown". A word
# that is obviously not an interface name beats inventing `can0`, which
# would read as a real capture from a real interface ever after.
UNKNOWN_IFACE = "unknown"


def candump_line(row: dict) -> str:
    """One archive row -> one line of candump ASCII.

        (1502979132.106111) slcan0 09F50374#000A00FFFF00FFFF

        line  := "(" epoch "." usec ") " iface " " canid "#" data
        epoch := 1*DIGIT      -- whole seconds since 1970-01-01T00:00:00Z
        usec  := 6DIGIT       -- zero-padded, exactly 6
        iface := 1*(ALPHA / DIGIT)
        canid := 8HEXDIG      -- uppercase, extended (29-bit) frame
        data  := 0*16HEXDIG   -- uppercase, even length, 0-8 bytes

    Nothing is decoded on the way out any more than on the way in: `raw`
    already IS `<canid>#<data>`, so this re-cases it, pads the identifier
    to the full 29-bit width and puts the timestamp back into the shape
    candump wrote it in. What the frame MEANS is canboat's business.

    Refuses a row of any other protocol rather than emitting something
    canboat would misread: an 0183 sentence has no CAN identifier, and the
    grammar has no way to say so.
    """
    proto = row.get("proto")
    if proto != "n2k":
        raise ValueError(f"candump is CAN frames; {proto!r} has no CAN identifier")

    # The two halves are formatted separately, which is exact by
    # construction. Formatting `ts.timestamp()` as one float would also work
    # today — but only because a double still has room to spare at current
    # POSIX timestamps: adjacent doubles sit 2.4e-7 s apart in 2026, which
    # is fine against a microsecond, and that gap doubles at every power of
    # two (4.8e-7 in 2038, 9.5e-7 in 2106, 1.9e-6 after that, where %.6f
    # starts rounding to the wrong microsecond). Nothing here should depend
    # on that argument being re-checked.
    ts = datetime.fromisoformat(row["ts"])
    if ts.tzinfo is None:
        # .timestamp() would read it as local time and shift the whole
        # export by the exporting machine's offset, silently.
        raise ValueError(f"ts carries no timezone: {row['ts']!r}")
    epoch = int(ts.replace(microsecond=0).timestamp())

    id_hex, sep, data_hex = row["raw"].partition("#")
    if not sep:
        raise ValueError(f"raw is not <canid>#<data>: {row['raw']!r}")
    can_id = int(id_hex, 16)
    # Validated by decoding: bytes.fromhex refuses an odd length and any
    # non-hex digit, so a corrupt row is skipped by the caller rather than
    # written out as a line canboat will read as a different frame.
    data = bytes.fromhex(data_hex)
    if len(data) > 8:
        raise ValueError(f"{len(data)} data bytes; a CAN frame carries at most 8")

    iface = row.get("src") or UNKNOWN_IFACE
    return (f"({epoch}.{ts.microsecond:06d}) {iface} "
            f"{can_id:08X}#{data.hex().upper()}")


def make_writer(out, fmt: str, fieldnames: list[str]):
    """Return a write_row(row) callable for the chosen format. candump
    writes no header and ignores fieldnames — its line is fixed by the
    format canboat reads, not by this archive's columns. CSV writes
    a header immediately and flattens any nested value (just _log's
    `details`, currently) to a JSON string, since a CSV cell can't hold a
    nested structure. ndjson writes one json.dumps line per row and keeps
    nested values as-is — the whole reason to prefer --format ndjson for
    _log specifically."""
    if fmt == "csv":
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()

        def write_row(row: dict) -> None:
            flat = {k: (json.dumps(v, sort_keys=True) if isinstance(v, dict) else v)
                    for k, v in row.items()}
            writer.writerow(flat)
        return write_row

    if fmt == "ndjson":
        def write_row(row: dict) -> None:
            out.write(json.dumps(row, separators=(",", ":")) + "\n")
        return write_row

    if fmt == "candump":
        def write_row(row: dict) -> None:
            out.write(candump_line(row) + "\n")
        return write_row

    raise ValueError(f"unknown format: {fmt!r}")


def export_source(s3, bucket: str, source: str, proto: str | None,
                   since: date, until: date,
                   output_path: Path | None, fmt: str, verbose: bool) -> tuple[int, int, int]:
    """Export one source to output_path (or stdout if None). Returns
    (objects_read, objects_failed, rows_written); the caller turns a
    non-zero objects_failed into exit code 2."""
    fieldnames = FIELDS[source]
    read_rows = READERS[source]

    # Atomic write: build the file under a sibling .tmp name and rename it
    # into place only once the export has actually finished. Without this,
    # a process kill or a network-mount hiccup mid-run (e.g. writing to a
    # remote share) leaves a truncated file sitting at the real output
    # path with no sign it's incomplete — same tmp-then-rename reasoning
    # as the disk spool in logger.py.
    tmp_path = output_path.with_name(output_path.name + ".tmp") if output_path else None
    out = open(tmp_path, "w", newline="", encoding="utf-8") if tmp_path else sys.stdout

    try:
        write_row = make_writer(out, fmt, fieldnames)

        objects_read = 0
        objects_failed = 0
        rows_written = 0
        keys = (iter_log_keys(s3, bucket, since, until) if source == "_log"
                else iter_keys(s3, bucket, since, until, proto))
        for key in keys:
            # A network failure (with its own retries) happens before any
            # row is yielded, so that case leaves zero rows from this key
            # in the output. Skip and keep going rather than aborting the
            # whole export over one bad object: this is a read-only
            # analysis tool, not a writer of the archive, so a
            # best-effort result (clearly reported) beats losing everything
            # to one persistent failure — and that applies just as much to
            # a malformed object (corrupt gzip, invalid JSON, a row with a
            # field FIELDS doesn't expect) as to a network error, so both
            # are caught and skipped the same way.
            try:
                row_count = 0
                for row in read_rows(s3, bucket, key):
                    write_row(row)
                    row_count += 1
                objects_read += 1
                rows_written += row_count
                if verbose:
                    print(f"  {key} ({row_count} rows)", file=sys.stderr)
            except (BotoCoreError, ClientError) as e:
                objects_failed += 1
                print(f"  SKIPPED {key} — retries exhausted: {e}", file=sys.stderr)
            except Exception as e:
                # Not a network failure — malformed/corrupt content
                # (bad gzip, invalid JSON, an unexpected field). Same
                # best-effort handling: skip this object, keep going.
                # Unlike the network case, rows already written from
                # THIS key before the bad row was hit stay in the output.
                objects_failed += 1
                print(f"  SKIPPED {key} — unreadable/malformed content: {e}", file=sys.stderr)
    except BaseException:
        # Anything that escapes the per-object handling above (disk full,
        # Ctrl-C, a real bug) means the export didn't finish — discard the
        # partial .tmp rather than leave it looking like a real file.
        if tmp_path:
            out.close()
            tmp_path.unlink(missing_ok=True)
        raise
    else:
        if tmp_path:
            out.flush()
            os.fsync(out.fileno())
            out.close()
            tmp_path.rename(output_path)

    label = source if proto is None else f"{source}/{proto}"
    print(f"[{label}] Done: {objects_read} object(s), {rows_written} row(s) written"
          f"{' to ' + str(output_path) if output_path else ''}."
          f"{f' {objects_failed} object(s) SKIPPED after retries exhausted — export is incomplete.' if objects_failed else ''}",
          file=sys.stderr)

    # No audit-log entry. This tool reads; it changes nothing, and `_log/`
    # exists to answer "what changed the archive, when". Writing an entry
    # per export also meant the one read-only tool here was the one
    # mutating the bucket — permanently, since these credentials cannot
    # delete — every time someone piped an export into jq. Failures and
    # skipped objects go to stderr, which the operator running this
    # interactively is already watching, and still set exit code 2.
    return objects_read, objects_failed, rows_written


def main():
    parser = argparse.ArgumentParser(prog="nmea2s3-exporter", description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=sorted(FIELDS), default="raw",
                         help="Which archive to read: `raw` (captured traffic, the default) or "
                              "`_log` (the operational audit log). They have different field sets — see SCHEMA.md")
    parser.add_argument("--proto", metavar="NAME", default=None,
                         help="With --source raw, export only this protocol (e.g. n2k, n0183). "
                              "Filtered on the object NAME, so nothing else is downloaded at all — "
                              "which matters because the protocols differ in volume by orders of "
                              "magnitude. Default: every protocol, interleaved in one stream")
    parser.add_argument("--format", choices=["candump", "csv", "ndjson"], default="ndjson",
                         help="Output format (default: ndjson). _log's `details` field is nested — kept as "
                              "real nested JSON in ndjson, flattened to a JSON string per cell in csv. "
                              "candump writes `(epoch.usec) iface CANID#DATA`, what `candump -L` writes "
                              "and canboat reads (`... --format candump | canboat convert`); it is CAN "
                              "frames only, so it implies --proto n2k")
    parser.add_argument("--since", type=parse_date, default=None, metavar="YYYY-MM-DD",
                         help="First UTC date to include (default: everything — no lower bound)")
    parser.add_argument("--until", type=parse_date, default=None, metavar="YYYY-MM-DD",
                         help="Last UTC date (inclusive) to include (default: everything — no upper bound)")
    parser.add_argument("-o", "--output", nargs="?", const="", default=None, metavar="PATH",
                         help="Write output to this file instead of stdout. Bare -o with no PATH auto-names "
                              "the file <timestamp>-<source>.<format> in the current directory")
    parser.add_argument("-v", "--verbose", action="store_true",
                         help="Print one line per object read to stderr, not just the final summary")
    parser.add_argument("--version", action="version", version=f"nmea2s3-exporter {__version__}")
    args = parser.parse_args()
    if args.proto and args.source != "raw":
        parser.error("--proto only applies to --source raw; the audit log has no protocol")
    if args.format == "candump":
        # A candump line is one CAN frame, so the export has to be one
        # protocol whatever the user asked for. Narrowing it here rather
        # than dropping rows later keeps the promise --proto already makes:
        # what cannot be written is never downloaded.
        if args.source != "raw":
            parser.error("--format candump reads captured frames; --source _log has none")
        if args.proto not in (None, "n2k"):
            parser.error(f"--format candump is CAN frames; --proto {args.proto} has none")
        args.proto = "n2k"
    config = load_config()

    since = args.since or date.min
    until = args.until or date.max

    if args.output is None:
        output_path = None
    elif args.output == "":
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        what = args.proto or args.source
        output_path = Path(f"{stamp}-{what}.{args.format}")
    else:
        output_path = Path(args.output)

    s3 = make_s3_client(config["s3_endpoint_url"], config["s3_region"],
                        config["s3_access_key_id"], config["s3_secret_access_key"])
    _read, objects_failed, _rows = export_source(
        s3, config["s3_bucket"], args.source, args.proto, since, until,
        output_path, args.format, args.verbose)

    # Exit 2 = the export completed but is INCOMPLETE: at least one object
    # could not be read after retries. This was previously recorded only as
    # a field inside an audit-log entry, so the process still exited 0 and
    # no caller could act on it — SCHEMA.md documented an exit code that
    # did not exist. A real code is what a shell or a cron wrapper can
    # actually branch on.
    if objects_failed:
        sys.exit(2)


if __name__ == "__main__":
    # Writes no audit entry: `_log/` records what CHANGED the archive. A
    # failed read changed nothing, and a traceback on the terminal of the
    # person who just ran it is the right channel.
    main()
