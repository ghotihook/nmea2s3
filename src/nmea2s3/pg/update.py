#!/usr/bin/env python3
"""nmea2s3-update-pg — archive objects -> a wide Postgres table.

Reads captured objects out of S3, decodes them, buckets them in time, and
upserts one wide row per bucket into Postgres. The table is created if it
does not exist and gains a column whenever a field appears that it has never
seen.

    raw/2026/08/24/120000-n2k-<cid>.ndjson.gz
        -> observations
           ts                  | n2k_sog | n2k_windangle_apparent | mwv_wind_angle_r
           2026-08-24 12:00:00 |    6.41 |                 -34.20 |           -33.90

`observations` is what the instruments SAID: every field, every device, no
judgement. It is not the table to query day to day — sql/metrics.sql builds
`metrics_1s` on top of it, which is what the instrument chains resolve to
and what a dashboard should read.

The bucket and the table name move independently. Nothing enforces that they
agree, so a non-default bucket wants a table saying so (`--bucket 5m --table
observations_5m`) — otherwise the `metrics_1s` view above it is named for a
grain it no longer has.

COLUMNS ARE `proto_field`, ONE PER DECODED FIELD
------------------------------------------------
`n2k_sog`, `rmc_spd_over_grnd`, `vtg_spd_over_grnd_kts` are three columns,
not one. There is deliberately no arbitration step choosing a winner between
COLUMNS: this table records what each instrument actually said. Preferring a
GPS's speed over a paddlewheel's is a question about your boat, it changes
over time, and it is answerable in SQL over these rows — whereas a value
discarded at write time is gone from a table that is supposed to be the
queryable form of the archive.

Two devices reporting the SAME field are a different question, because they
share one column and one of them has to be in it. That is settled by N2K
priority, then source address — see bucket.py.

ONE DEVICE PER BUCKET, ITS LAST SAMPLE, NOT A MEAN
--------------------------------------------------
Each bucket resolves each field to one device — lowest priority number, then
lowest source address — and takes that device's latest sample in the bucket.
See bucket.py for why that is the right primitive for an auto-widening table
with a configurable bucket, and for what it costs.

EXACTLY-ONCE, BY OBJECT
-----------------------
S3 keys are content-addressed and immutable, so a key never changes meaning
and "have I already ingested this?" is answerable by name. A ledger table
records every key consumed, and a normal run skips them. That is what makes
this safe to run on a cron every minute over an overlapping window: a late
spool upload is simply a new key appearing, picked up on the next pass.

`--rebuild` ignores the ledger and reprocesses everything in range. It is
safe because every write is an upsert keyed on ts.

THE DATABASE IS DERIVED AND DISPOSABLE
--------------------------------------
Everything here can be regenerated from the archive, which is why the table
is allowed to alter its own schema. Do not point this at a database holding
anything that cannot be rebuilt.

Required environment variables (see env.example):
  NMEA2S3_S3_ENDPOINT_URL, NMEA2S3_S3_BUCKET,
  NMEA2S3_S3_ACCESS_KEY_ID, NMEA2S3_S3_SECRET_ACCESS_KEY
  NMEA2S3_PG_HOST, NMEA2S3_PG_PORT, NMEA2S3_PG_DBNAME,
  NMEA2S3_PG_USER, NMEA2S3_PG_PASSWORD

Optional: NMEA2S3_S3_REGION (default us-east-1)

Install: pipx install git+https://github.com/ghotihook/nmea2s3.git — one
install, all three commands. This one needs psycopg, nmea2000 and pynmea2;
the logger imports none of them.
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timezone

from .. import __version__
from ..audit_log import log_action_safely
from ..ndjson import (iter_keys, iter_rows_ndjson_gz, key_date,
                      make_s3_client, required_env)

from . import bucket as bucket_mod
from . import table as table_mod

APPLICATION = "nmea2s3-update-pg"

DEFAULT_TABLE = "observations"
DEFAULT_BUCKET = "1s"

log = logging.getLogger("nmea2s3.pg")

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS {ledger} (
    key        TEXT PRIMARY KEY,
    ingested   TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_ts   TIMESTAMPTZ,
    last_ts    TIMESTAMPTZ,
    rows       INTEGER
)
"""


def load_config() -> dict:
    # Deferred until after argparse, so --help and --version work on a box
    # that has never been configured.
    return {
        "s3_endpoint_url": required_env("NMEA2S3_S3_ENDPOINT_URL"),
        "s3_bucket": required_env("NMEA2S3_S3_BUCKET"),
        "s3_region": os.environ.get("NMEA2S3_S3_REGION", "us-east-1"),
        "s3_access_key_id": required_env("NMEA2S3_S3_ACCESS_KEY_ID"),
        "s3_secret_access_key": required_env("NMEA2S3_S3_SECRET_ACCESS_KEY"),
        "pg_host": required_env("NMEA2S3_PG_HOST"),
        "pg_port": int(os.environ.get("NMEA2S3_PG_PORT", "5432")),
        "pg_dbname": required_env("NMEA2S3_PG_DBNAME"),
        "pg_user": required_env("NMEA2S3_PG_USER"),
        "pg_password": required_env("NMEA2S3_PG_PASSWORD"),
    }


def parse_date(text: str) -> date:
    return datetime.fromisoformat(text).date()


def ensure_ledger(con, ledger: str) -> None:
    table_mod.check_name(ledger)
    con.execute(LEDGER_DDL.format(ledger=ledger))


def ledger_keys(con, ledger: str) -> set[str]:
    return {r[0] for r in con.execute(f"SELECT key FROM {ledger}")}


def record_key(con, ledger: str, key: str, rows: list) -> None:
    con.execute(
        f"INSERT INTO {ledger} (key, first_ts, last_ts, rows) "
        f"VALUES (%s, %s, %s, %s) ON CONFLICT (key) DO NOTHING",
        (key, rows[0]["ts"] if rows else None,
         rows[-1]["ts"] if rows else None, len(rows)))


def ingest_object(s3, s3_bucket: str, key: str, buckets) -> int:
    """Decode one object into `buckets`. Returns the number of rows read.

    The object's rows are fed in the order they were written, which is
    capture order: wire_n2k reassembles fast-packet PGNs across frames and
    holds partial state, so anything else silently drops multi-frame PGNs.
    """
    count = 0
    for record in iter_rows_ndjson_gz(s3, s3_bucket, key):
        ts_text = record.get("ts")
        if not ts_text:
            continue
        try:
            record["_ts"] = datetime.fromisoformat(ts_text)
        except ValueError:
            continue
        buckets.add(record)
        count += 1
    return count


def run(args, config) -> int:
    # Imported here rather than at module scope only so that --help and
    # --version do not pay for a database driver.
    import psycopg

    bucket_size = bucket_mod.parse_interval(args.bucket)
    since = args.since or date.min
    until = args.until or datetime.now(timezone.utc).date()

    s3 = make_s3_client(config["s3_endpoint_url"], config["s3_region"],
                        config["s3_access_key_id"], config["s3_secret_access_key"])
    con = psycopg.connect(
        host=config["pg_host"], port=config["pg_port"], dbname=config["pg_dbname"],
        user=config["pg_user"], password=config["pg_password"],
        connect_timeout=10, autocommit=True)

    try:
        ensure_ledger(con, args.ledger)
        done = set() if args.rebuild else ledger_keys(con, args.ledger)

        keys = [k for k in iter_keys(s3, config["s3_bucket"], since, until, args.proto)
                if k not in done]
        keys.sort(key=lambda k: (key_date(k), k.rsplit("/", 1)[-1]))
        if not keys:
            print("Nothing to do: no unprocessed objects in range.", file=sys.stderr)
            return 0

        print(f"{len(keys)} object(s) to process, bucket={args.bucket}, "
              f"table={args.table}{' (rebuild)' if args.rebuild else ''}",
              file=sys.stderr)

        # One Buckets per object, not one for the whole run. A run can cover
        # months, and holding every bucket in it would grow without bound;
        # per-object keeps peak memory at one object's worth of buckets and
        # makes the ledger entry mean "this object is fully written".
        #
        # Rows spanning an object boundary are handled by the upsert: the
        # second object's bucket row updates the first's rather than
        # replacing it, column by column.
        total_rows = total_objects = total_dropped = 0
        new_columns: list[str] = []
        for key in keys:
            buckets = bucket_mod.Buckets(bucket_size)
            read = ingest_object(s3, config["s3_bucket"], key, buckets)
            rows = buckets.rows()
            total_dropped += buckets.dropped_out_of_range

            if args.dry_run:
                print(f"  [dry-run] {key}: {read} record(s) -> {len(rows)} row(s), "
                      f"{len(buckets.fields())} field(s)", file=sys.stderr)
                total_objects += 1
                total_rows += len(rows)
                continue

            new_columns += table_mod.ensure(con, args.table, buckets.fields())
            written = table_mod.write(con, args.table, rows)
            record_key(con, args.ledger, key, rows)
            total_objects += 1
            total_rows += written
            if args.verbose:
                print(f"  {key}: {read} record(s) -> {written} row(s)", file=sys.stderr)

        summary = (f"{total_objects} object(s), {total_rows} row(s) into "
                   f"{args.table} at {args.bucket}")
        if total_dropped:
            summary += f", {total_dropped} sample(s) dropped out of range"
        if new_columns:
            summary += f", {len(new_columns)} new column(s): {', '.join(new_columns)}"
        print(f"Done: {summary}.", file=sys.stderr)

        if args.dry_run:
            print("This was a DRY RUN — nothing was written to Postgres.",
                  file=sys.stderr)
        elif new_columns or args.rebuild:
            # Only runs that did something no later reader could reconstruct.
            #
            # WHICH objects were ingested, and how many rows each produced, is
            # already in the ledger table — per object, written in the loop
            # above. An entry per incremental run duplicates that into a
            # bucket whose credentials cannot delete: the logger lands a new
            # object every FLUSH_INTERVAL (300 s, ~288 a day), so any cron
            # under ~5 minutes finds new work almost every time and wrote
            # ~288 permanent entries a day. Thousands a month, burying the
            # handful that matter.
            #
            # These two do not survive the ledger being dropped, which is the
            # whole reason to write here instead of relying on Postgres — it
            # is derived and disposable, and rebuilding it loses the fact that
            # a column ever appeared or that a rebuild was ever ordered:
            #
            #   new_columns   a schema change. The table means something
            #                 different after this run than before it
            #   --rebuild     the ledger was deliberately ignored and rows
            #                 rewritten, so two readings of the same range
            #                 disagreeing has a recorded cause
            #
            # A routine catch-up run is not silent, just not permanent: the
            # summary above still goes to stderr, and to the journal with it.
            log_action_safely(
                s3, config["s3_bucket"], APPLICATION, 0, f"updated pg: {summary}",
                {"objects": total_objects, "rows": total_rows, "table": args.table,
                 "bucket": args.bucket, "proto": args.proto,
                 "rebuild": args.rebuild, "new_columns": new_columns,
                 "dropped_out_of_range": total_dropped})
        return 0
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(
        prog="nmea2s3-update-pg", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, metavar="INTERVAL",
                         help=f"Time bucket, e.g. 1s, 250ms, 5m, 1h (default: {DEFAULT_BUCKET})")
    parser.add_argument("--table", default=DEFAULT_TABLE, metavar="NAME",
                         help=f"Destination table, created if absent (default: {DEFAULT_TABLE})")
    parser.add_argument("--ledger", default=None, metavar="NAME",
                         help="Table recording ingested object keys "
                              "(default: <table>_objects)")
    parser.add_argument("--proto", default=None, metavar="NAME",
                         help="Only ingest this protocol (e.g. n2k, n0183). Filtered on "
                              "the object name, so nothing else is downloaded. "
                              "Default: every protocol")
    parser.add_argument("--since", type=parse_date, default=None, metavar="YYYY-MM-DD",
                         help="First UTC date to consider (default: the whole archive)")
    parser.add_argument("--until", type=parse_date, default=None, metavar="YYYY-MM-DD",
                         help="Last UTC date, inclusive (default: today)")
    parser.add_argument("--rebuild", action="store_true",
                         help="Ignore the ledger and reprocess every object in range. "
                              "Safe: every write is an upsert keyed on ts")
    parser.add_argument("--dry-run", action="store_true",
                         help="Read and decode, report what would be written, touch "
                              "no table. Note this still downloads every object")
    parser.add_argument("-v", "--verbose", action="store_true",
                         help="One line per object, not just the summary")
    parser.add_argument("--log-level", default="INFO",
                         choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"))
    parser.add_argument("--version", action="version",
                         version=f"nmea2s3-update-pg {__version__}")
    args = parser.parse_args()
    if args.ledger is None:
        args.ledger = f"{args.table}_objects"

    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stderr)
    # Checked before anything connects or downloads, so a typo costs nothing.
    try:
        bucket_mod.parse_interval(args.bucket)
    except ValueError as e:
        parser.error(str(e))

    sys.exit(run(args, load_config()))


if __name__ == "__main__":
    main()
