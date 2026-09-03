"""The 0183 importer: the record it produces, and the correctness model.

Every test here is behavioural — it drives the real functions and asserts on
what they produced. None of it asserts on source text.

The importer's whole claim is that re-running it is free: it keeps no state
file, so every run recomputes each day's content id and reconciles against
what is actually in the bucket. That only holds if identical rows produce
identical bytes, and these credentials cannot delete — so a run that drifts
does not fail, it silently accumulates a second copy of every day, forever.
"""

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import helpers as H                                              # noqa: E402
from nmea2s3 import migrate_n0183 as M                           # noqa: E402
from nmea2s3.ndjson import key_proto, record_line                # noqa: E402

DAY = date(2026, 8, 24)


def _rows(n=3, device="boat-pi", src="10.0.0.7"):
    """(received_at, device_id, source_ip, raw_data) as psycopg would yield."""
    t0 = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)
    return [(t0 + timedelta(seconds=i), device, src,
             f"$GPRMC,12000{i},A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*6A")
            for i in range(n)]


class _Conn:
    """The two things export_day() asks of a connection: a named (server-side)
    cursor used as a context manager, and rows out of it."""

    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def cursor(self, name=None):
        return _Cursor(self)


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self.itersize = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, query, params):
        self.conn.queries.append((query, params))

    def __iter__(self):
        return iter(self.conn.rows)


def _export(rows, s3=None, dry_run=False, device_filter=None):
    s3 = s3 or H.FakeS3()
    count, outcome = M.export_day(_Conn(rows), "raw_n0183", DAY, device_filter,
                                  s3, "b", dry_run, False)
    return s3, count, outcome


# ── the record ───────────────────────────────────────────────────────────

def test_a_migrated_row_is_the_same_record_the_logger_writes():
    """One shape for every protocol, whatever produced it. An imported row and
    a captured one differ in their VALUES, never in their fields — that is
    what lets a reader dispatch on `proto` instead of on provenance."""
    received, device, src, raw = _rows(1)[0]
    imported = json.loads(M.row_to_line(received, device, src, raw))
    captured = json.loads(record_line(H.T0, 1.5, "boat-pi", "can0", "n2k", "x#00"))
    assert list(imported) == list(captured)
    assert imported["proto"] == "n0183"
    assert imported["raw"] == raw
    assert imported["device_id"] == device
    assert imported["src"] == src


def test_mono_is_null_and_never_invented():
    """`ts - mono` dates the boot epoch, and is meaningful only if mono was
    genuinely read from the same machine at the same moment. These rows were
    captured by a process that never read CLOCK_MONOTONIC; a computed value
    here would produce a boot epoch that looks real and is fiction."""
    received, device, src, raw = _rows(1)[0]
    assert json.loads(M.row_to_line(received, device, src, raw))["mono"] is None


def test_a_naive_source_timestamp_cannot_reach_the_archive():
    """UTC-aware or refused, the same rule every writer here is held to."""
    naive = datetime(2026, 8, 24, 12, 0, 0)
    try:
        M.row_to_line(naive, "boat-pi", "10.0.0.7", "$GPRMC,120000,A*6A")
        assert False, "a naive timestamp should be refused"
    except (ValueError, TypeError):
        pass


# ── the correctness model: same rows, same key, every run ────────────────

def test_the_same_rows_produce_the_same_key_every_run():
    """No state file: a re-run recomputes the day and reconciles against the
    bucket. If the bytes drift, every run uploads a fresh duplicate under a
    new key that these credentials cannot delete."""
    a, _, _ = _export(_rows(5))
    b, _, _ = _export(_rows(5))
    assert list(a.puts) == list(b.puts)
    assert list(a.puts.values()) == list(b.puts.values())


def test_a_day_already_in_the_bucket_is_not_uploaded_again():
    s3, count, outcome = _export(_rows(4))
    assert (count, outcome) == (4, "uploaded")
    again_count, again_outcome = M.export_day(_Conn(_rows(4)), "raw_n0183", DAY,
                                              None, s3, "b", False, False)
    assert (again_count, again_outcome) == (4, "up-to-date")
    assert len(s3.puts) == 1, "a second object for unchanged data"


def test_changed_source_data_lands_beside_the_old_object_not_over_it():
    """A day holding two objects is a real signal that its source changed
    after an earlier export — not something to paper over by picking a
    winner, and not something these credentials could clean up anyway."""
    s3, _, _ = _export(_rows(4))
    _export(_rows(5), s3=s3)
    assert len(s3.puts) == 2


def test_the_key_is_the_archive_key_for_this_protocol():
    s3, _, _ = _export(_rows(2))
    key = next(iter(s3.puts))
    assert key.startswith("raw/2026/08/24/")
    assert key.endswith(".ndjson.gz")
    assert key_proto(key) == "n0183"


def test_a_day_with_no_rows_writes_nothing():
    s3, count, outcome = _export([])
    assert (count, outcome) == (0, "empty")
    assert not s3.puts


def test_a_dry_run_reports_what_would_change_and_writes_nothing():
    """Without --live every run is a dry run, so an unfamiliar or copy-pasted
    command cannot silently write to a bucket nothing can delete from."""
    s3, count, outcome = _export(_rows(3), dry_run=True)
    assert (count, outcome) == (3, "uploaded")
    assert not s3.puts, "a dry run reached the bucket"


def test_the_rows_are_ordered_before_they_are_hashed():
    """Ordering is what makes the content id reproducible: raw_n0183 has no
    primary key, so rows tying on received_at are free to come back in any
    order unless the query pins them. The ORDER BY has to cover every column
    that reaches the OUTPUT, since those are what the hash is taken over."""
    conn = _Conn(_rows(2))
    M.export_day(conn, "raw_n0183", DAY, None, H.FakeS3(), "b", True, False)
    sent = conn.queries[0][0].as_string(None) if hasattr(
        conn.queries[0][0], "as_string") else str(conn.queries[0][0])
    ordered = sent.split("ORDER BY", 1)[1]
    for column in ("received_at", "device_id", "raw_data", "source_ip"):
        assert column in ordered, f"{column} reaches the output but not ORDER BY"
