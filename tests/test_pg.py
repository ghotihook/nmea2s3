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
    from nmea2s3.pg import bucket, table, wire_n2k
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


def _system_time(prio: int, src: int, when: datetime, source: int = 0) -> str:
    """A PGN 126992 (System Time) frame carrying a GPS clock.

    `source` is the frame's own clock source, 0 = GPS. Anything else is the
    capture box's own clock read back — a local crystal, say — which must not
    reach the column a reader compares `ts` against.
    """
    can_id = (prio << 26) | (0x1F010 << 8) | src
    days = (when.date() - datetime(1970, 1, 1, tzinfo=timezone.utc).date()).days
    secs = when.hour * 3600 + when.minute * 60 + when.second
    payload = (bytes([0x01, source]) + days.to_bytes(2, "little")
               + (secs * 10000).to_bytes(4, "little"))
    return f"{can_id:08x}#{payload.hex()}"


def _temperature(pgn: int, payload: bytes, prio: int = 5, src: int = 2) -> str:
    """One of the three temperature PGNs, in the candump form the archive
    stores. Built from the real wire layout — the whole point is that the
    library names the same quantity differently per PGN."""
    return f"{((prio << 26) | (pgn << 8) | src):08x}#{payload.hex()}"


def _t130312(kelvin: float) -> str:
    """Temperature (DEPRECATED). sid, instance, source, actual/set at 0.01 K."""
    return _temperature(0x1FD08, bytes([0x01, 0x00, 0x00])
                        + round(kelvin * 100).to_bytes(2, "little")
                        + round(kelvin * 100).to_bytes(2, "little") + b"\xff")


def _t130316(kelvin: float) -> str:
    """Temperature Extended Range — what replaced 130312, so what a current
    instrument pack actually sends. actual at 0.001 K, set at 0.1 K."""
    return _temperature(0x1FD0C, bytes([0x01, 0x00, 0x00])
                        + round(kelvin * 1000).to_bytes(3, "little")
                        + round(kelvin * 10).to_bytes(2, "little"))


def _t130310(water_k: float, air_k: float) -> str:
    """Environmental Parameters. water and air at 0.01 K, pressure at 100 Pa."""
    return _temperature(0x1FD06, bytes([0x01])
                        + round(water_k * 100).to_bytes(2, "little")
                        + round(air_k * 100).to_bytes(2, "little")
                        + (1013).to_bytes(2, "little") + b"\xff")


def _fast_packet(can_id: int, data: bytes, seq: int = 0) -> list[str]:
    """A fast-packet PGN split into the frames it actually travels as.

    Six payload bytes in the first frame behind a counter and a length, seven
    in each one after. Built properly rather than faked, because wire_n2k
    reassembles across calls and holds partial state — a test feeding one
    whole message would not exercise the thing most likely to break.
    """
    out = [bytes([(seq << 5), len(data)]) + data[:6]]
    rest, n = data[6:], 0
    while rest:
        n += 1
        out.append(bytes([(seq << 5) | n]) + rest[:7])
        rest = rest[7:]
    return [f"{can_id:08x}#{f.hex()}" for f in out]


def _gnss_position(prio: int, src: int, gnss_type: int = 1,
                    method: int = 2, integrity: int = 1) -> list[str]:
    """PGN 129029 (GNSS Position Data), the fix and how good it is.

    gnssType, method and integrity are LOOKUPs — numbers whose meanings live
    in a table. method 2 is `DGNSS fix`, integrity 1 is `Safe`.
    """
    data = bytearray(43)
    data[31] = ((method & 0x0F) << 4) | (gnss_type & 0x0F)
    data[32] = integrity & 0x03
    can_id = (prio << 26) | (1 << 24) | (0xF8 << 16) | (0x05 << 8) | src
    return _fast_packet(can_id, bytes(data))


def _rmc(when: datetime) -> str:
    """An RMC sentence reporting `when` as its own UTC date and time."""
    body = (f"GPRMC,{when:%H%M%S},A,4807.038,N,01131.000,E,"
            f"022.4,084.4,{when:%d%m%y},003.1,W")
    return _nmea(body)


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


# ── choosing a device, then its last sample ──────────────────────────────

def test_the_bucket_keeps_the_last_sample_not_an_average():
    b = bucket.Buckets(timedelta(seconds=1))
    for i, angle in enumerate((10.0, 20.0, 30.0)):
        b.add(_rec(T0 + timedelta(milliseconds=100 * i), "n0183",
                   _nmea(f"IIMWV,{angle:05.1f},R,10.5,N,A")))
    rows = b.rows()
    assert len(rows) == 1
    assert rows[0]["mwv_wind_angle_r"] == 30.0, "last, not the mean of 20.0"


def test_the_best_priority_wins_the_bucket_it_reported_in():
    """Two devices reporting one field land in one column, so the bucket has
    to pick one. Priority first, and not merely as a tie-break on the exact
    timestamp: the priority-1 sample here is the EARLIEST of the three."""
    b = bucket.Buckets(timedelta(seconds=1))
    for offset, prio, src, sog in ((0, 1, 9, 6.4), (100, 3, 5, 2.0), (200, 7, 2, 9.0)):
        b.add(_rec(T0 + timedelta(milliseconds=offset), "n2k", _n2k(prio, src, sog)))
    assert abs(b.rows()[0]["n2k_sog"] - 6.4) < 0.01, \
        "lowest priority number wins, whatever reported afterwards"


def test_equal_priority_falls_to_the_lowest_source_address():
    """Same priority, so the sort reaches its second key — again before time
    is consulted: the winning sample is the earlier one."""
    b = bucket.Buckets(timedelta(seconds=1))
    for offset, src, sog in ((0, 2, 6.4), (100, 9, 2.0)):
        b.add(_rec(T0 + timedelta(milliseconds=offset), "n2k", _n2k(1, src, sog)))
    assert abs(b.rows()[0]["n2k_sog"] - 6.4) < 0.01, "lower source address wins"


def test_the_winning_device_contributes_its_own_latest_sample():
    """`last` still means last — within the device the first two keys chose.
    The column is one instrument's reading at a known moment, never a value
    assembled from two of them."""
    b = bucket.Buckets(timedelta(seconds=1))
    for offset, prio, src, sog in ((0, 1, 9, 5.0), (100, 3, 5, 2.0), (200, 1, 9, 6.4)):
        b.add(_rec(T0 + timedelta(milliseconds=offset), "n2k", _n2k(prio, src, sog)))
    assert abs(b.rows()[0]["n2k_sog"] - 6.4) < 0.01, \
        "the better device's later sample, not its first"


def test_a_better_device_cannot_pin_a_bucket_it_missed():
    """The bound on how stale the rule can make a column: nothing crosses a
    bucket boundary, so a priority-1 device that reports at 0.2 Hz wins only
    the buckets it actually appears in. The rest belong to the next best,
    outright."""
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "n2k", _n2k(1, 9, 6.4)))                       # bucket 1
    b.add(_rec(T0 + timedelta(seconds=1), "n2k", _n2k(3, 5, 2.0)))  # bucket 2, alone
    first, second = b.rows()
    assert abs(first["n2k_sog"] - 6.4) < 0.01
    assert abs(second["n2k_sog"] - 2.0) < 0.01, \
        "a device absent from a bucket cannot win it"


def test_the_latest_sample_wins_between_equally_ranked_talkers():
    """0183 carries neither priority nor source address, so its talkers are
    all equal and the sort falls through to time — where `last` is the whole
    rule. Three XDR talkers share this archive, so this is the real case."""
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


# ── units ────────────────────────────────────────────────────────────────

def test_every_temperature_is_stored_in_celsius_whatever_pgn_it_came_from():
    """N2K carries every temperature in Kelvin and the library names the same
    quantity differently per PGN, so CONVERSIONS is keyed on names that do
    not generalise. Until 2026-09-03 it listed only `actualTemperature`,
    which meant the DEPRECATED PGN was the one that worked: a current pack
    sending 130316 stored 293.15 where an old one stored 20.0, under a
    column name that gives no hint which it is.

    Asserted over every temperature column rather than the four known field
    ids, so a PGN nobody has looked at yet fails here rather than in the
    archive."""
    twenty_c = 293.15
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "n2k", _t130312(twenty_c)))
    b.add(_rec(T0, "n2k", _t130316(twenty_c)))
    b.add(_rec(T0, "n2k", _t130310(twenty_c, 298.15)))
    row = b.rows()[0]

    temps = {k: v for k, v in row.items() if "temperature" in k or "temp" in k}
    assert temps, "no temperature columns at all — the frames did not decode"
    for column, value in temps.items():
        assert value < 100, f"{column} = {value} is Kelvin, not Celsius"

    # The three PGNs reported the same physical temperature, so the columns
    # they produce must agree — that is the thing a reader relies on.
    for column in ("n2k_actualtemperature_0_sea_temperature",
                   "n2k_temperature_0_sea_temperature",
                   "n2k_watertemperature"):
        assert column in row, f"{column} missing from {sorted(row)}"
        assert abs(row[column] - 20.0) < 0.05, f"{column} = {row[column]}"


def test_the_sea_temperature_guard_would_not_drop_a_real_reading():
    """ranges.py bounds sea temperature at (0, 40), which is a Celsius bound.
    Applied to a column the conversion missed it would reject 293.15 on every
    single reading — dropping the entire feed rather than the impossible
    values it exists to catch. So the bound and the conversion have to cover
    the same set of columns."""
    from nmea2s3.pg import ranges
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "n2k", _t130316(293.15)))
    b.add(_rec(T0, "n2k", _t130310(293.15, 298.15)))
    row = b.rows()[0]

    for column, value in row.items():
        if column in ranges.RANGES and isinstance(value, (int, float)):
            assert ranges.in_range(column, value), \
                f"{column} = {value} is a real reading its own bound rejects"


# ── the GPS clock, as an ordinary column ─────────────────────────────────

def test_the_gps_clock_reaches_a_column_from_both_protocols():
    """`ts` is CLOCK_REALTIME and only as good as the capture box's clock.
    The archive's independent check on it is the GPS clock riding inside the
    data, so it has to survive decoding as an ordinary value — the whole
    point being that a reader compares the two in SQL.

    It nearly did not. Both protocols carry it, but the n2k side reached a
    column for neither: `date` and `time` come back from the library as
    datetime objects, and wire_n2k keeps a field only when its value is an
    int or a float, so PGN 126992 — whose entire purpose is a clock —
    contributed nothing at all.
    """
    gps = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "n2k", _system_time(3, 2, gps)))
    b.add(_rec(T0, "n0183", _rmc(gps)))
    row = b.rows()[0]

    n2k_col = "n2k_gps_time_gps"          # `source` is a discriminator
    assert n2k_col in row, f"no n2k GPS clock in {sorted(row)}"
    assert "rmc_gps_time" in row, f"no 0183 GPS clock in {sorted(row)}"
    assert row[n2k_col] == row["rmc_gps_time"] == gps.timestamp(), \
        "both protocols report the same instant, so both columns must agree"


def test_a_local_crystal_clock_is_not_a_gps_clock():
    """126992's source may be the capture box's own clock. Comparing `ts`
    against that is circular — it would always agree, and say nothing — so it
    must not land in the column a reader trusts as a GPS reference."""
    gps = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "n2k", _system_time(3, 2, gps, source=2)))   # local crystal
    row = b.rows()[0] if b.rows() else {}
    assert "n2k_gps_time_gps" not in row, \
        f"a non-GPS source reached the GPS column: {sorted(row)}"


def test_the_capture_clock_is_recorded_against_the_gps_clock_never_judged():
    """Nothing here drops or rejects a row over a clock disagreement. The
    logger records what it was given, this records the GPS clock beside it,
    and how far apart is too far is a question answered in SQL — where it can
    be changed — not one frozen into the table."""
    gps = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)
    wrong = gps + timedelta(days=400)        # a badly wrong capture clock
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(wrong, "n2k", _system_time(3, 2, gps)))
    rows = b.rows()
    assert len(rows) == 1, "a disagreeing clock must not drop the row"
    assert rows[0]["ts"] == bucket.truncate(wrong, timedelta(seconds=1)), \
        "the row is still bucketed by its own capture ts, unrepaired"
    assert rows[0]["n2k_gps_time_gps"] == gps.timestamp(), \
        "and carries the GPS clock beside it, so the two can be compared"


def test_two_gps_units_are_arbitrated_like_any_other_field():
    """The GPS clock is an ordinary reading, so it carries its frame's own
    priority and source address and settles by the normal rule — something
    the 0183 side cannot do, since RMC has neither."""
    early = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)
    late = datetime(2026, 8, 24, 12, 0, 5, tzinfo=timezone.utc)
    b = bucket.Buckets(timedelta(seconds=1))
    b.add(_rec(T0, "n2k", _system_time(5, 1, late)))     # worse priority
    b.add(_rec(T0 + timedelta(milliseconds=10), "n2k", _system_time(2, 9, early)))
    assert b.rows()[0]["n2k_gps_time_gps"] == early.timestamp(), \
        "the better-priority device wins the bucket, later sample or not"


# ── lookups ──────────────────────────────────────────────────────────────

def test_a_lookup_reaches_a_column_as_its_code():
    """A LOOKUP is a number whose meanings live in a table. The library
    resolves it to text, which is not a DOUBLE PRECISION, so every one of
    them used to be dropped on the floor — taking `method`, the GNSS fix
    quality, with it. Exactly what you reach for when a position looks wrong.

    The code is stored, never the resolved text: it is the raw reading, and
    it leaves the enum where a wrong entry is a fix rather than something
    frozen into rows that are never rewritten.
    """
    b = bucket.Buckets(timedelta(seconds=1))
    for raw in _gnss_position(3, 2, gnss_type=1, method=2, integrity=1):
        b.add(_rec(T0, "n2k", raw))
    row = b.rows()[0]
    assert row["n2k_method_code"] == 2, "DGNSS fix is code 2"
    assert row["n2k_gnsstype_code"] == 1
    assert row["n2k_integrity_code"] == 1
    assert all(isinstance(v, (int, float)) for k, v in row.items() if k != "ts"), \
        "a column is DOUBLE PRECISION — no resolved text may reach one"


def test_frame_metadata_lookups_do_not_become_columns():
    """manufacturerCode and industryCode head 314 and 313 PGNs between them,
    and say nothing about anything measured. A column each would bury the
    handful of lookups worth having, so they are skipped by name — the lookup
    equivalent of `sid`."""
    from nmea2000.consts import FieldTypes
    from nmea2000.message import NMEA2000Field

    def field(fid, value, raw):
        return NMEA2000Field(fid, fid, None, None, value, raw, None,
                             FieldTypes.LOOKUP, False)

    class FakeMsg:
        PGN = 130824
        fields = [field("manufacturerCode", "Simrad", 1857),
                  field("industryCode", "Marine Industry", 4),
                  field("mode", "Automatic", 3)]

    saved = wire_n2k._decoder
    wire_n2k._decoder = type("D", (), {"decode": staticmethod(lambda line: FakeMsg())})()
    try:
        _disc, values = wire_n2k.decode_frame(0x09F80102, "00")
    finally:
        wire_n2k._decoder = saved

    assert values == {"mode_code": 3.0}, \
        f"only the measured lookup should survive, got {values}"


# ── the table, driven through a fake connection ──────────────────────────

class FakeCon:
    """Records SQL. `columns` is what the catalogue will claim exists.

    `elsewhere` is a same-named table in ANOTHER schema, which only a query
    matching on the bare name can see — see the two branches below, and
    test_a_same_named_table_in_another_schema_cannot_suppress_a_column.
    """

    def __init__(self, columns=(), elsewhere=()):
        self.sql = []
        self.columns = list(columns)
        self.elsewhere = list(elsewhere)
        self.copied = []

    def execute(self, q, params=None):
        self.sql.append(" ".join(q.split()))
        if "to_regclass" in q:
            # search_path resolves to ONE relation, whatever else shares its
            # name.
            return [(c,) for c in self.columns]
        if "information_schema.columns" in q:
            # Matched on `table_name` alone, so every schema's table of that
            # name answers at once.
            return [(c,) for c in self.columns + self.elsewhere]
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
    assert any("ALTER TABLE observations ADD COLUMN IF NOT EXISTS "
               "mwv_wind_angle_r DOUBLE PRECISION" in s for s in con.sql)
    assert not any("ADD COLUMN n2k_sog" in s for s in con.sql), \
        "an existing column must not be re-added"


def test_a_same_named_table_in_another_schema_cannot_suppress_a_column():
    """The DDL resolves through search_path to one relation; the lookup that
    decides whether to emit it must resolve the same way.

    `information_schema.columns WHERE table_name = %s` did not — it matched
    the bare name in every schema and returned the union, so an old
    `public.observations` holding a column made `ensure` skip adding that
    column to the `"$user".observations` actually being written. The COPY
    then failed with `column does not exist`, which is the precise failure
    `ensure` exists to prevent, and it repeated every run because the union
    never changed.
    """
    con = FakeCon(columns=["ts"], elsewhere=["n2k_sog"])
    added = table.ensure(con, "observations", ["n2k_sog"])
    assert added == ["n2k_sog"], "a column this table lacks must still be added"
    assert any("ADD COLUMN" in s and "n2k_sog" in s for s in con.sql)


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
