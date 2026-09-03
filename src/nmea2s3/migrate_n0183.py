"""Sync raw_n0183 rows from Postgres into the nmea2s3 archive in S3.

Historical import for the 0183 side of the archive: a one-way bridge out of
the pre-nmea2s3 Postgres schema, kept here only while there is still old data
to move. When there is not, delete this module and its entry point — nothing
else imports it.

It writes the SAME record and the SAME key layout as the live logger because
it calls the same functions, and it lives in this package so that stays true
by construction:

    raw/<yyyy>/<mm>/<dd>/<HHMMSS>-n0183-<content_id>.ndjson.gz

    {"ts":...,"mono":null,"device_id":...,"src":...,"proto":"n0183","raw":...}

Sharing that code is the point, and being in the same package is the
strongest available form of it. An earlier version of this script lived in
another repo with its own copy of the record builder and the key format, and
the two drifted — exactly what a permanent archive cannot afford. There is
now one implementation of the format, with this package's own test suite
standing behind it.

WHAT CHANGED FROM THE OLD SCHEMA
--------------------------------
The five-column 0183 record collapsed into the six-field envelope every
protocol now shares:

    received_at   -> ts        (same meaning; `received_at` described the
                                old UDP-listener architecture)
    device_id     -> device_id
    source_ip     -> src       (generalised: an input name, which for a
                                network capture path is the peer address)
    raw_data      -> raw       (verbatim, unchanged)
    sentence_type -> DROPPED   (it is a substring of the sentence;
                                nmea2s3.decode.n0183() recovers it)
    -             -> mono      (null: these rows predate any monotonic
                                clock and one must never be invented)
    -             -> proto     ("n0183")

IDEMPOTENCE, AND THE THREE WAYS TO LOSE IT
------------------------------------------
No state file, deliberately — every run rechecks the FULL requested range
from scratch. Rather than trust a local file that a day was handled once,
this queries Postgres, recomputes that day's content_id, and reconciles
against what is actually in S3:

  - exact key already present -> content matches, nothing to do
  - key not present           -> upload it

That only works if the same rows produce the same bytes every time, and
three ordinary-looking mistakes break it. Each produces a DIFFERENT
content_id for unchanged data, so every run uploads a fresh duplicate
object that these credentials cannot delete:

  1. UNORDERED READS. raw_n0183 has no primary key, so rows tying on
     received_at are free to come back in any order. The ORDER BY has to
     cover every column that reaches the OUTPUT, since those are what the
     hash is taken over — not because ties are likely, but because the
     whole correctness model is that recomputing the hash gives the same
     answer, and that must not depend on luck.
  2. NON-DETERMINISTIC BATCHING. If a re-run chunks rows differently, every
     object boundary moves and every key changes. Fixed by construction
     here: exactly one object per day, always, whatever the row count.
  3. FORMATTING DRIFT. Float and timestamp rendering has to be identical
     run to run. This is the strongest reason to call nmea2s3's
     record_line() rather than build JSON locally.

Nothing is ever deleted: this issues no delete, and the credential it runs
under does not need to be able to. If a day's source data genuinely changes
between runs, the new content gets its own key and is uploaded alongside
the old one. Both persist. A day holding more than one object is a real,
visible signal that its source changed after an earlier export, not
something to paper over by picking a winner.

The credential DOES need read, though, which the logger's does not. The
reconciliation above is a HEAD per day (object_exists), and HEAD is
s3:GetObject: with a PUT+LIST-only key a MISSING object answers 404 and a
PRESENT one answers 403 AccessDenied — which is non-retryable, so it
propagates and ends the run. That is the second-run case, the one the whole
no-state-file design is built around, so it would fail exactly when the
reconciliation was doing its job. Run this with a key that can list, read
and put; delete is neither needed nor wanted. (Reconciling with a
list_objects_v2 on the day prefix instead would drop the read requirement —
worth doing if this ever has to run under the logger's own key.)

A day is only checked once fully in the past (received_at < now - lag), so
late-arriving rows for an in-progress day don't add a new object for the
same still-growing day.

Required environment variables (see env.example):
  NMEA2S3_SRC_PG_HOST, NMEA2S3_SRC_PG_PORT, NMEA2S3_SRC_PG_DBNAME,
  NMEA2S3_SRC_PG_USER, NMEA2S3_SRC_PG_PASSWORD
  NMEA2S3_S3_ENDPOINT_URL, NMEA2S3_S3_BUCKET,
  NMEA2S3_S3_ACCESS_KEY_ID, NMEA2S3_S3_SECRET_ACCESS_KEY

  SRC_PG, and deliberately not the NMEA2S3_PG_* that nmea2s3-update-pg
  uses. Those name two different databases pointing opposite ways: this
  one READS a legacy capture table that is the last copy of that data;
  update-pg WRITES a derived table it is free to drop and rebuild. One
  set of variables for both would let a mistyped host point an importer
  at the derived database, or a rebuild at the source.

Optional: NMEA2S3_S3_REGION (default us-east-1; DO Spaces ignores it,
boto3 requires it)
"""

import argparse
import os
import secrets
import sys
from datetime import date, datetime, timedelta, timezone

import psycopg
from psycopg import sql

from . import __version__
from .audit_log import log_action_safely
from .ndjson import (gzip_and_id_stream, make_s3_client, object_exists,
                     put_object_gz, record_line, required_env, s3_key)
from .retry import with_retries

# Named for the console script, like every other tool here — and it is now a
# path segment in every `_log/` key this writes. See audit_log.py.
APPLICATION = "nmea2s3-migrate-n0183"

PG_TABLE = "raw_n0183"
PROTO = "n0183"
DEFAULT_LAG_SECONDS = 300


def load_config() -> dict:
    # Deferred until after argparse handles --help, so that does not require
    # a working env file — only actually running a sync does.
    return {
        "pg_host": required_env("NMEA2S3_SRC_PG_HOST"),
        "pg_port": int(required_env("NMEA2S3_SRC_PG_PORT")),
        "pg_dbname": required_env("NMEA2S3_SRC_PG_DBNAME"),
        "pg_user": required_env("NMEA2S3_SRC_PG_USER"),
        "pg_password": required_env("NMEA2S3_SRC_PG_PASSWORD"),
        "s3_endpoint_url": required_env("NMEA2S3_S3_ENDPOINT_URL"),
        "s3_bucket": required_env("NMEA2S3_S3_BUCKET"),
        "s3_region": os.environ.get("NMEA2S3_S3_REGION", "us-east-1"),
        "s3_access_key_id": required_env("NMEA2S3_S3_ACCESS_KEY_ID"),
        "s3_secret_access_key": required_env("NMEA2S3_S3_SECRET_ACCESS_KEY"),
    }


def parse_date(s: str) -> date:
    return datetime.fromisoformat(s).date()


def row_to_line(received_at, device_id, source_ip, raw_data) -> str:
    """One Postgres row as one archive record.

    Deliberately thin: every formatting decision that could drift between
    this importer and the live logger — timestamp rendering, field order,
    JSON separators, the UTC check — belongs to record_line(), which both
    call. All this function does is name which column means which field.

    `mono` is None, not a computed value. These rows were captured by a
    process that never read CLOCK_MONOTONIC, and `ts - mono` is only
    meaningful if `mono` was genuinely read from the same machine at the
    same moment. Inventing one would produce a boot epoch that looks real
    and is fiction.

    `sentence_type` is not passed and not stored — it is a substring of
    raw_data, recoverable with nmea2s3.decode.n0183().

    A naive `received_at` is REFUSED rather than converted. datetime
    .astimezone() on a naive value does not fail: it assumes the local zone
    and returns an aware value, so a `timestamp without time zone` column
    imported from a machine set to Australia/Sydney files every row ten
    hours early — and record_line()'s UTC check cannot catch it, because by
    then the value is aware and wrong rather than naive. It would depend on
    the timezone of whichever machine happened to run the import, be
    invisible in the output, and land in an archive these credentials
    cannot delete from. If the source column really is naive, the zone it
    was recorded in is a fact only the operator has; say it in the query
    (`received_at AT TIME ZONE '...'`) rather than letting this guess.
    """
    if received_at.tzinfo is None or received_at.tzinfo.utcoffset(received_at) is None:
        raise ValueError(
            f"received_at {received_at!r} has no timezone. Refusing to guess: "
            "astimezone() would silently assume this machine's local zone. "
            "Cast it in the source query, e.g. "
            "received_at AT TIME ZONE 'UTC' AT TIME ZONE 'UTC'")
    return record_line(
        ts=received_at.astimezone(timezone.utc),
        mono=None,
        device_id=device_id,
        src=str(source_ip) if source_ip is not None else None,
        proto=PROTO,
        raw=raw_data,
    )


def earliest_data_date(conn, table: str) -> date:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT min(received_at) FROM {}").format(sql.Identifier(table)))
        (min_ts,) = cur.fetchone()
        if min_ts is None:
            raise SystemExit(f"{table} is empty; nothing to export")
        if min_ts.tzinfo is None or min_ts.tzinfo.utcoffset(min_ts) is None:
            # Same refusal as row_to_line(), and for the same reason:
            # .astimezone() on a naive value assumes this machine's local
            # zone rather than failing. Unchecked here it picks a start_day
            # shifted by the runner's offset, writes that range into the
            # start audit entry, and only then fails on the first row.
            raise SystemExit(
                f"{table}.received_at has no timezone. Refusing to guess a "
                "start date: cast it in the source query, e.g. "
                "received_at AT TIME ZONE 'UTC' AT TIME ZONE 'UTC'")
        return min_ts.astimezone(timezone.utc).date()


def export_day(conn, table: str, day: date, device_filter, s3, bucket: str,
               dry_run: bool, verbose: bool) -> tuple[int, str]:
    """Query, checksum, and reconcile one day against what is already in S3.

    Always queries Postgres and recomputes the day's content_id — that is
    the cost of genuinely verifying idempotency every run rather than
    assuming it from a state file. Never deletes anything, nor could it.
    Returns (row_count, outcome) where outcome is "empty", "up-to-date" or
    "uploaded".
    """
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    where = ["received_at >= %s", "received_at < %s"]
    params = [day_start, day_end]
    if device_filter:
        placeholders = ",".join(["%s"] * len(device_filter))
        where.append(f"device_id IN ({placeholders})")
        params.extend(device_filter)

    # ORDER BY covers every column that reaches the OUTPUT — see mistake (1)
    # in the module docstring. That is the condition the hash actually
    # depends on: two rows differing only in a column nobody writes produce
    # identical lines, so their relative order cannot change the content_id.
    #
    # sentence_type is ordered on but no longer selected, which Postgres
    # allows for a plain query. It is belt-and-braces now rather than
    # load-bearing: it was part of the old five-column record, and keeping
    # it in the ordering costs nothing and pins the row order completely
    # rather than merely enough.
    query = sql.SQL("""
        SELECT received_at, device_id, source_ip, raw_data
        FROM {table}
        WHERE {where}
        ORDER BY received_at, device_id, raw_data, sentence_type, source_ip
    """).format(table=sql.Identifier(table), where=sql.SQL(" AND ".join(where)))

    total_rows = 0
    first_ts = None

    def stream_lines():
        """Yield the day's rows, counting and noting the first timestamp on
        the way past. A server-side cursor keeps Postgres from materializing
        the day either."""
        nonlocal total_rows, first_ts
        with conn.cursor(name=f"n0183_export_{day.isoformat()}") as cur:
            cur.itersize = 20000
            cur.execute(query, params)
            for received_at, device_id, source_ip, raw_data in cur:
                if first_ts is None:
                    first_ts = received_at
                total_rows += 1
                yield row_to_line(received_at, device_id, source_ip, raw_data)

    # One pass: hash and compress together, holding only the compressed
    # output. Building a list of every line for the day AND the string
    # joining them was an estimated 6GB+ peak on a high-rate day.
    cid, body = gzip_and_id_stream(stream_lines())

    if total_rows == 0:
        if verbose:
            print(f"{day.isoformat()}: 0 rows, nothing to do", file=sys.stderr)
        return total_rows, "empty"

    time_of_day = first_ts.astimezone(timezone.utc).strftime("%H%M%S")
    key = s3_key(PROTO, day.strftime("%Y%m%d"), time_of_day, cid)

    if object_exists(s3, bucket, key):
        if verbose:
            print(f"{day.isoformat()}: {total_rows} rows, already up to date ({key})",
                  file=sys.stderr)
        return total_rows, "up-to-date"

    if dry_run:
        print(f"{day.isoformat()}: [dry-run] would upload {key} ({total_rows} rows)",
              file=sys.stderr)
        return total_rows, "uploaded"

    # Retried in-process: the key is content-addressed, so a retry after a
    # transient failure resolves to the same key and is a safe overwrite,
    # never a duplicate. If every attempt fails this propagates up and fails
    # the run; nothing was written for this day, so the next invocation
    # rechecks it cleanly rather than leaving a silent gap.
    with_retries(put_object_gz, s3, bucket, key, body, what=f"upload {key}")
    if verbose:
        print(f"{day.isoformat()}: wrote {key} ({total_rows} rows, {len(body)} bytes gzip)",
              file=sys.stderr)
    return total_rows, "uploaded"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table", default=PG_TABLE, metavar="NAME",
                         help=f"Source Postgres table to read (default: {PG_TABLE})")
    parser.add_argument("--since", type=parse_date, default=None, metavar="YYYY-MM-DD",
                         help="First UTC date to check (default: earliest data in the table — "
                              "the whole archive, every run, since there is no state file)")
    parser.add_argument("--until", type=parse_date, default=None, metavar="YYYY-MM-DD",
                         help="Last UTC date (inclusive) to check (default: today, subject to --lag-seconds)")
    parser.add_argument("--lag-seconds", type=int, default=DEFAULT_LAG_SECONDS, metavar="N",
                         help="Only check days fully older than now minus this lag, so a still-arriving "
                              f"day does not churn (default: {DEFAULT_LAG_SECONDS})")
    parser.add_argument("--device-id", action="append", default=None, metavar="ID",
                         help="Restrict export to this device_id. Repeatable. Filters which rows are "
                              "included; no effect on the S3 key, which carries no device_id")
    parser.add_argument("--live", action="store_true",
                         help="Actually write to S3. Without this every run is a dry run — query "
                              "Postgres and report what WOULD change — so an unfamiliar or "
                              "copy-pasted command cannot silently write")
    parser.add_argument("--version", action="version",
                         version=f"nmea2s3-migrate-n0183 {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true",
                         help="Print one line per day checked, not just days with something to report")
    args = parser.parse_args()

    dry_run = not args.live
    print("DRY RUN — nothing will be written to S3." if dry_run else "LIVE RUN — writing to S3.",
          file=sys.stderr)
    config = load_config()

    conn = psycopg.connect(
        host=config["pg_host"], port=config["pg_port"], dbname=config["pg_dbname"],
        user=config["pg_user"], password=config["pg_password"], connect_timeout=10,
    )
    try:
        start_day = args.since or earliest_data_date(conn, args.table)

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=args.lag_seconds)
        # A day D is "closed" once cutoff is at or past its end, i.e. midnight
        # of D+1 <= cutoff. That holds for any D <= cutoff.date() - 1.
        last_closeable_day = cutoff.date() - timedelta(days=1)
        end_day = min(args.until or last_closeable_day, last_closeable_day)

        s3 = make_s3_client(config["s3_endpoint_url"], config["s3_region"],
                            config["s3_access_key_id"], config["s3_secret_access_key"])

        if start_day > end_day:
            print(f"Nothing to do: start_day={start_day} is after end_day={end_day}",
                  file=sys.stderr)
            return 0

        # A live run writes a start entry BEFORE touching anything and an end
        # entry when it finishes — always both, paired by run_id. This run
        # takes over an hour, so "did it finish?" has to be answerable from
        # the bucket: a start with no matching end means it died partway
        # (killed, dropped connection, closed terminal), which no
        # exception handler can catch because nothing is ever raised.
        #
        # The end entry is NOT gated on having uploaded anything. A live run
        # that found every day already correct changed nothing but did
        # complete, and "completed, nothing to do" must stay distinguishable
        # from "died before finishing". A dry run writes neither: it never
        # touches S3, so there is no run to account for.
        run_id = secrets.token_hex(4)
        if not dry_run:
            log_action_safely(
                s3, config["s3_bucket"], APPLICATION, 0,
                f"import STARTED: {start_day} to {end_day} ({args.table} -> raw/ as {PROTO})",
                {"event": "start", "run_id": run_id, "since": str(start_day),
                 "until": str(end_day), "table": args.table, "proto": PROTO},
            )

        day, days_checked, days_uploaded, total_rows = start_day, 0, 0, 0
        failure = None
        try:
            while day <= end_day:
                rows, outcome = export_day(conn, args.table, day, args.device_id, s3,
                                            config["s3_bucket"], dry_run, args.verbose)
                total_rows += rows
                days_checked += 1
                days_uploaded += (outcome == "uploaded")
                day += timedelta(days=1)
                # End the read transaction each day. psycopg opens one at the
                # first execute() and holds it until close, so a full import
                # left the SOURCE database idle in transaction for hours,
                # holding off autovacuum on a production table this tool
                # describes as the last copy of its data. rollback() rather
                # than commit() because nothing here writes to Postgres.
                conn.rollback()
        except BaseException as e:
            # A run that writes some days and then dies is a fact worth
            # keeping in the bucket, so the end entry is written either way —
            # with the exit code and the day it got to. Then it re-raises:
            # the traceback belongs on the terminal of whoever ran it.
            #
            # BaseException, not Exception: Ctrl-C is the single likeliest way
            # an hour-long import ends early, and KeyboardInterrupt is not an
            # Exception. `finally` ran regardless, so catching the narrower
            # type recorded an interrupted run as `exit_code: 0` and "import
            # FINISHED ... {start_day} to {end_day}" — a matched start/end
            # pair claiming a range it never reached, which is precisely the
            # lie the pairing exists to make impossible.
            failure = e
            raise
        finally:
            if not dry_run:
                log_action_safely(
                    s3, config["s3_bucket"], APPLICATION, 1 if failure else 0,
                    (f"import FAILED at {day}: {failure}" if failure else
                     f"import FINISHED: uploaded {days_uploaded} day(s) of {days_checked} "
                     f"checked ({start_day} to {end_day}), {total_rows} row(s) total"),
                    {"event": "end", "run_id": run_id, "row_count": total_rows,
                     "days_checked": days_checked, "days_uploaded": days_uploaded,
                     "since": str(start_day), "until": str(end_day),
                     "table": args.table, "proto": PROTO,
                     **({"failed_on": str(day)} if failure else {})},
                )

        print(f"Done: {days_checked} day(s) checked, {days_uploaded} uploaded, "
              f"{total_rows} row(s) total.", file=sys.stderr)
        if dry_run:
            print("This was a DRY RUN — re-run with --live to actually write.", file=sys.stderr)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
