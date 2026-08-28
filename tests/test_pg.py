"""The wide Postgres table: bucketing, `last()`, and a schema that grows.

Skips rather than fails when the decoders are absent, which happens when the
suite is run straight out of a working tree with nothing pip-installed. A
skipped module is reported by name; a silently reduced suite would be worse
than a failing one.

Nothing here talks to a database. `table.py` is driven through a fake
connection that records the SQL it was given, which is the point: what this
module has to get right is the DDL and the upsert, and both are strings.
"""

import os
import struct
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import helpers as H                                              # noqa: E402

try:
    from nmea2s3.pg import bucket, table
except ImportError as e:                                          # pragma: no cover
    raise unittest.SkipTest(f"decoder stack not importable: {e}")

T0 = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)


def _nmea(body: str) -> str:
    """`IIMWV,045.0,R,10.5,N,A` -> a sentence with a REAL checksum.

    Not cosmetic. pynmea2 validates any checksum that is present, whatever
    its `check` flag says, so a sentence with a made-up `*00` decodes to
    nothing at all — which is correct behaviour and made an early version of
    these tests silently assert on empty buckets.
    """
    checksum = 0
    for ch in body:
        checksum ^= ord(ch)
    return f"${body}*{checksum:02X}"


def _n2k(prio: int, src: int, sog_kn: float) -> str:
    """A PGN 129026 (COG/SOG rapid) frame from one device, in the candump
    form the archive stores. Priority and source address live in the CAN
    identifier, which is exactly what the tie-break sorts on — so varying
    them here varies the thing under test rather than a stand-in for it.
    """
    can_id = (prio << 26) | (0x1F802 << 8) | src
    payload = (bytes([0xFF, 0xFC])                       # SID, COG reference
               + struct.pack("<HH", 0, round(sog_kn / 1.94384 / 0.01))
               + b"\xff\xff")
    return f"{can_id:08x}#{payload.hex()}"


def _rec(ts, proto, raw):
    return {"ts": ts.isoformat(), "_ts": ts, "proto": proto, "raw": raw,
            "device_id": "boat-pi", "src": None, "mono": None}


# ── the bucket grid ──────────────────────────────────────────────────────

def test_intervals_parse_the_way_the_flag_documents_them():
    assert bucket.parse_interval("1s") == timedelta(seconds=1)
    assert bucket.parse_interval("250ms") == timedelta(milliseconds=250)
    assert bucket.parse_interval("5m") == timedelta(minutes=5)
    assert bucket.parse_interval("1h") == timedelta(hours=1)
    for bad in ("1", "s", "0s", "-5s", "1 s", "1sec", "fortnight"):
        try:
            bucket.parse_interval(bad)
            assert False, f"{bad!r} should be rejected"
        except ValueError:
            pass


def test_buckets_are_anchored_to_the_epoch_not_to_the_batch():
    """Two runs over overlapping ranges must produce the same boundaries, or
    the same second lands under two keys and the upsert stops converging."""
    odd = T0.replace(second=37, microsecond=400000)
    assert bucket.truncate(odd, timedelta(seconds=1)).second == 37
    assert bucket.truncate(odd, timedelta(seconds=1)).microsecond == 0
    assert bucket.truncate(odd, timedelta(minutes=5)).minute % 5 == 0
    assert bucket.truncate(odd, timedelta(minutes=5)).second == 0
    # the boundary does not depend on where the batch started
    later = odd + timedelta(hours=3)
    for size in ("1s", "250ms", "5m", "1h"):
        step = bucket.parse_interval(size)
        a, b = bucket.truncate(odd, step), bucket.truncate(later, step)
        assert (b - a).total_seconds() % step.total_seconds() == 0, size


# ── last(), and what breaks ties ─────────────────────────────────────────

def test_the_bucket_keeps_the_last_sample_not_an_average():
    b = bucket.Buckets(timedelta(seconds=1))
    for i, angle in enumerate((10.0, 20.0, 30.0)):
        b.add(_rec(T0 + timedelta(milliseconds=100 * i), "n0183",
                   _nmea(f"IIMWV,{angle:05.1f},R,10.5,N,A")))
    rows = b.rows()
    assert len(rows) == 1
    assert rows[0]["mwv_wind_angle_r"] == 30.0, "last, not the mean of 20.0"


def test_a_tie_on_time_is_broken_by_priority_then_source():
    """Two devices reporting one field land in one column, and samples can
    share a timestamp. Without a tie-break the winner is whichever order the
    dict happened to see them in, and the table stops being reproducible.

    Drives real frames: this test carried a copy of the rule written out in
    its own body until 2026-08-29, so it passed whatever bucket.py did.
    """
    b = bucket.Buckets(timedelta(seconds=1))
    for prio, src, sog in ((3, 5, 2.0), (1, 9, 6.4), (7, 2, 9.0)):
        b.add(_rec(T0, "n2k", _n2k(prio, src, sog)))
    assert abs(b.rows()[0]["n2k_sog"] - 6.4) < 0.01, \
        "lowest priority number wins a tie on ts, whatever the arrival order"


def test_a_tie_on_time_and_priority_falls_to_the_source_address():
    """Same priority, so the sort has to reach its third key. Source
    addresses are leased by ISO address claiming and change when the bus is
    repowered — sorting by one is stable, choosing a device by one is not."""
    b = bucket.Buckets(timedelta(seconds=1))
    for src, sog in ((9, 2.0), (2, 6.4)):
        b.add(_rec(T0, "n2k", _n2k(1, src, sog)))
    assert abs(b.rows()[0]["n2k_sog"] - 6.4) < 0.01, "lower source address wins"


def test_a_later_sample_beats_a_better_priority():
    """`last` means last. Priority only settles a tie ON TIME — otherwise a
    high-priority device that stopped reporting would pin the column
    forever."""
    b = bucket.Buckets(timedelta(seconds=10))
    b.add(_rec(T0, "n0183", _nmea("IIMWV,010.0,R,10.5,N,A")))
    b.add(_rec(T0 + timedelta(seconds=1), "n0183", _nmea("IIMWV,020.0,R,10.5,N,A")))
    assert b.rows()[0]["mwv_wind_angle_r"] == 20.0


def test_an_impossible_value_never_reaches_a_bucket():
    """Load-bearing under `last()` in a way it was not under a mean: a mean
    dilutes one wild reading, `last` can hand it the whole bucket. A real
    corrupt MWV sentence once reported 1003.1 knots."""
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "n0183", _nmea("IIMWV,045.0,R,10.5,N,A")))
    b.add(_rec(T0 + timedelta(milliseconds=1), "n0183",
               _nmea("IIMWV,045.0,R,1003.1,N,A")))
    assert b.rows()[0]["mwv_wind_speed_r"] == 10.5
    assert b.dropped_out_of_range == 1


def test_a_corrupt_sentence_contributes_nothing():
    """pynmea2 validates a checksum whenever one is present, so corruption
    on the wire never reaches a column. The archive still holds the raw
    sentence — this drops it from the derived table only."""
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "n0183", _nmea("IIMWV,045.0,R,10.5,N,A")))
    b.add(_rec(T0 + timedelta(milliseconds=1), "n0183",
               "$IIMWV,999.0,R,88.8,N,A*00"))      # deliberately wrong checksum
    assert b.rows()[0]["mwv_wind_angle_r"] == 45.0, "the corrupt one must not win"


def test_an_unknown_protocol_is_skipped_not_fatal():
    """The archive is allowed to grow a protocol this build has never heard
    of. A reader that falls over on one is worse than one that skips it."""
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "seatalk", "9c11XX"))
    b.add(_rec(T0, "n0183", _nmea("IIMWV,045.0,R,10.5,N,A")))
    assert b.rows()[0]["mwv_wind_angle_r"] == 45.0


def test_column_names_are_proto_field():
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "n0183", _nmea("GPRMC,120000,A,3745.0,S,14512.0,E,6.4,84.4,230394,3.1,W")))
    b.add(_rec(T0, "n0183", _nmea("IIMWV,045.0,R,10.5,N,A")))
    fields = b.fields()
    assert "rmc_spd_over_grnd" in fields
    assert "mwv_wind_angle_r" in fields
    assert not any(f in fields for f in ("sog", "awa", "aws")), \
        "no arbitration: raw field ids are the columns, not resolved names"


# ── the table, driven through a fake connection ──────────────────────────

class FakeCon:
    """Records SQL. `columns` is what information_schema will claim exists."""

    def __init__(self, columns=()):
        self.sql = []
        self.columns = list(columns)
        self.copied = []

    def execute(self, q, params=None):
        self.sql.append(" ".join(q.split()))
        if "information_schema.columns" in q:
            return [(c,) for c in self.columns]
        return []

    def cursor(self):
        return self

    def copy(self, statement):
        self.sql.append(" ".join(statement.split()))
        con = self

        class Copier:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def write(self, text): con.copied.append(text)
        return Copier()


def test_the_table_is_created_when_absent():
    con = FakeCon()
    table.ensure(con, "observations", ["n2k_sog"])
    assert any("CREATE TABLE IF NOT EXISTS observations" in s for s in con.sql)
    assert any("PRIMARY KEY (ts)" in s for s in con.sql)


def test_a_missing_column_is_added():
    con = FakeCon(columns=["ts", "n2k_sog"])
    added = table.ensure(con, "observations", ["n2k_sog", "mwv_wind_angle_r"])
    assert added == ["mwv_wind_angle_r"]
    assert any("ALTER TABLE observations ADD COLUMN mwv_wind_angle_r DOUBLE PRECISION"
               in s for s in con.sql)
    assert not any("ADD COLUMN n2k_sog" in s for s in con.sql), \
        "an existing column must not be re-added"


def test_a_field_id_that_would_break_the_ddl_is_refused():
    """Column names arrive from a wire format, via a device on a bus. They
    are interpolated into DDL, so they are checked rather than trusted."""
    for bad in ("drop table x", "n2k-sog", "N2K_SOG", "2fast", "", "a" * 64,
                'n2k_sog"; DROP TABLE observations; --'):
        try:
            table.check_name(bad)
            assert False, f"{bad!r} should be refused"
        except ValueError:
            pass
    for good in ("ts", "n2k_sog", "mwv_wind_angle_r", "xdr_m5_heel", "a" * 63):
        table.check_name(good)


def test_the_upsert_only_touches_columns_this_batch_carries():
    """A narrow run must not blank columns it knows nothing about, which is
    what listing every column and writing NULL for the absent ones would
    do."""
    con = FakeCon(columns=["ts", "n2k_sog", "mwv_wind_angle_r"])
    table.write(con, "observations", [{"ts": T0, "n2k_sog": 6.4}])
    upsert = [s for s in con.sql if "ON CONFLICT" in s][0]
    assert "n2k_sog = EXCLUDED.n2k_sog" in upsert
    assert "mwv_wind_angle_r" not in upsert, \
        "a column absent from this batch must keep its value"


def test_a_missing_measurement_is_written_as_null_not_zero():
    con = FakeCon(columns=["ts", "a", "b"])
    table.write(con, "observations", [{"ts": T0, "a": 1.0}, {"ts": T0, "b": 2.0}])
    body = "".join(con.copied)
    assert body.splitlines()[0].endswith(","), "absent value is an empty CSV field"
    assert "0.0" not in body, "a gap must never become a zero reading"
