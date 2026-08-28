"""The on-disk and in-bucket formats, pinned.

These objects are the archive. They outlive every process that wrote them and
will be read by tools that do not exist yet, so the shape of a row, the name
of a key and the headers on an object are all interface, not implementation.

This module also checks the code against SCHEMA.md directly, so the two
cannot drift: a field renamed in one place and not the other fails here.
"""

import gzip
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import helpers as H                                              # noqa: E402
from nmea2s3 import export, logger as L                            # noqa: E402
import nmea2s3.audit_log as audit                                    # noqa: E402
from nmea2s3 import decode                                          # noqa: E402
from nmea2s3.ndjson import (GZIP_CONTENT_TYPE, key_proto, put_object_gz,   # noqa: E402
                            record_line, s3_key)

RAW_FIELDS = ["ts", "mono", "device_id", "src", "proto", "raw"]
LOG_FIELDS = ["timestamp", "application", "host", "exit_code", "comment", "details"]


def _n2k_row():
    return json.loads(H.frame(L, 0).line)


def _log_entry():
    captured = {}

    class Rec:
        def put_object(self, Bucket, Key, Body, **kw):
            captured.update(key=Key, body=Body, kw=kw)

    audit.log_action(Rec(), "b", "nmea2s3-logger", 0, "test", {"rx": 1})
    return captured


# ── field names and order ────────────────────────────────────────────────

def test_every_protocol_produces_exactly_the_same_fields_in_order():
    """The point of the unified record: one shape, so a reader dispatches on
    `proto` and never on which prefix a row came from."""
    n0183 = json.loads(record_line(H.T0, None, "boat-pi", "/dev/ttyUSB0",
                                    "n0183", "$GPRMC,120000,A,3745.1,S*6A"))
    assert list(_n2k_row()) == RAW_FIELDS
    assert list(n0183) == RAW_FIELDS


def test_log_entry_has_exactly_the_documented_fields():
    entry = json.loads(_log_entry()["body"])
    assert sorted(entry) == sorted(LOG_FIELDS)


# ── field types ──────────────────────────────────────────────────────────

def test_raw_field_types():
    """A reader casting these into a table needs the JSON types stable."""
    r = _n2k_row()
    assert isinstance(r["ts"], str) and r["ts"].endswith("+00:00")
    assert isinstance(r["mono"], float)
    for f in ("device_id", "src", "proto", "raw"):
        assert isinstance(r[f], str), f


def test_mono_is_null_not_invented_when_there_is_no_monotonic_clock():
    """A historical import out of a database has no monotonic reading to
    record. Null says so; a fabricated one would silently corrupt
    `ts - mono`, which is the only reason the field exists."""
    row = json.loads(record_line(H.T0, None, "boat-pi", None, "n0183", "$GPRMC*6A"))
    assert row["mono"] is None
    assert row["src"] is None, "an unknown source is null, never an empty string"


def test_a_proto_that_would_break_the_key_is_refused():
    """`-` and `_` delimit the object name and the spool filename, so a
    proto containing either produces a key that cannot be parsed back — and
    these credentials cannot delete it."""
    for bad in ("signalk-delta", "sea_talk", "N2K", "n2k#1", ""):
        try:
            record_line(H.T0, 1.0, "d", None, bad, "x")
            assert False, f"{bad!r} should be refused"
        except ValueError:
            pass
    for good in ("n2k", "n0183", "seatalk", "sk.delta"):
        record_line(H.T0, 1.0, "d", None, good, "x")


def test_the_derived_fields_survive_the_round_trip():
    """The archive stops storing pgn/src_addr/priority, so this is the
    check that they are genuinely recoverable rather than lost. Every frame
    class: PDU2, PDU1, broadcast, addressed."""
    for can_id in (0x09F80102, 0x0DF01203, 0x18EEFF00, 0x1CFD0805):
        payload = bytes.fromhex("deadbeefcafef00d")
        row = json.loads(L.build_frame(H.T0, 1.0, can_id, payload).line)
        frame = decode.n2k(row["raw"])
        assert frame.can_id == can_id
        assert frame.payload == payload
        assert (frame.pgn, frame.src_addr, frame.priority) == decode.decode_can_id(can_id)


def test_an_empty_payload_round_trips():
    """No DLC is stored — the payload length is the hex length — so a frame
    carrying no data has to survive as an empty payload, not as a parse
    error."""
    row = json.loads(L.build_frame(H.T0, 1.0, 0x09F80102, b"").line)
    assert row["raw"] == "09f80102#"
    assert decode.n2k(row["raw"]).payload == b""


def test_timestamps_are_tz_aware_utc_and_round_trip():
    for row in (_n2k_row(), json.loads(_log_entry()["body"])):
        ts = row.get("ts") or row["timestamp"]
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None, "never naive"
        assert parsed.utcoffset().total_seconds() == 0, "always UTC, never local"


def test_raw_is_lowercase_hex_of_the_whole_frame():
    """Identifier included — the frame as it came off the wire, not just its
    payload. Lowercase, so two writers cannot disagree on case and produce
    two content ids for one frame."""
    frame = L.build_frame(H.T0, 1.0, 0x09F80102, bytes.fromhex("DEADBEEFCAFEF00D"))
    assert json.loads(frame.line)["raw"] == "09f80102#deadbeefcafef00d"


# ── the code agrees with SCHEMA.md ───────────────────────────────────────

def _schema_tables():
    """Field names from each source's table in SCHEMA.md, keyed by section."""
    text = (H.REPO / "SCHEMA.md").read_text()
    out, current = {}, None
    for line in text.splitlines():
        m = re.match(r"^## `(\w+)`", line)
        if m:
            current = m.group(1)
            out[current] = []
        elif line.startswith("###"):
            # A subsection's table is about something else — `details`
            # contents, say — not a continuation of the source's field list.
            # Without this they were silently appended to it.
            current = None
        elif current:
            f = re.match(r"^\| `(\w+)`", line)
            if f:
                out[current].append(f.group(1))
    return out


def test_schema_md_documents_the_fields_the_code_actually_writes():
    """Docs drift silently. This is the check that makes them not."""
    tables = _schema_tables()
    assert tables.get("raw") == RAW_FIELDS, f"SCHEMA.md raw: {tables.get('raw')}"
    assert tables.get("_log") == LOG_FIELDS, f"SCHEMA.md _log: {tables.get('_log')}"


def test_the_exporter_reads_the_same_fields_the_writers_write():
    """The exporter names its columns explicitly, so a new field must be
    added there too or exports silently lose it."""
    assert export.FIELDS["raw"] == RAW_FIELDS
    assert export.FIELDS["_log"] == LOG_FIELDS


# ── keys and filenames ───────────────────────────────────────────────────

def test_data_key_layout():
    assert (s3_key("n2k", "20260824", "120000", "abc123def456")
            == "raw/2026/08/24/120000-n2k-abc123def456.ndjson.gz")
    assert (s3_key("n0183", "20231103", "004056", "d536016446d6")
            == "raw/2023/11/03/004056-n0183-d536016446d6.ndjson.gz")


def test_one_tree_so_a_day_is_a_single_prefix():
    """Both protocols share the date path. A reader asking for one day
    issues one listing, rather than merging one per protocol — it dispatches
    on each row's `proto` and never looks at the key."""
    a = s3_key("n2k", "20260824", "120000", "aaaa")
    b = s3_key("n0183", "20260824", "120004", "bbbb")
    assert a.rsplit("/", 1)[0] == b.rsplit("/", 1)[0] == "raw/2026/08/24"


def test_the_protocol_is_recoverable_from_the_key_alone():
    """What makes a narrowed export cheap: LIST returns names without
    bodies, so filtering happens before anything is downloaded."""
    for proto in ("n2k", "n0183", "seatalk"):
        assert key_proto(s3_key(proto, "20260824", "120000", "abc123")) == proto


def test_the_key_and_the_rows_agree_on_the_protocol():
    """`proto` is in the key AND on every row — deliberately redundant, so a
    row stays self-describing once separated from its key. Redundancy that
    is never checked is just two things free to disagree."""
    obj = L._objects([H.frame(L, 0)])[0]
    key = s3_key(L.PROTO, obj.day, obj.time_of_day, obj.cid)
    rows = [json.loads(l) for l in
            gzip.decompress(obj.body).decode().splitlines() if l]
    assert rows and all(r["proto"] == key_proto(key) for r in rows)


def test_data_key_is_day_partitioned_by_capture_time():
    """The path must reflect when the rows were CAPTURED, never when they were
    uploaded — that is what makes a delayed replay land in the right day."""
    key = s3_key("n2k", "20260824", "235959", "abc")
    assert key.split("/")[1:4] == ["2026", "08", "24"]


def test_log_key_layout():
    key = _log_entry()["key"]
    assert re.fullmatch(r"_log/\d{4}/\d{2}/\d{2}/\d{6}-[0-9a-f]{8}\.json", key), key


def test_log_key_is_not_content_addressed():
    """Two identical entries are two real events, so they must not collapse to
    one key the way data objects deliberately do."""
    a, b = _log_entry()["key"], _log_entry()["key"]
    assert a != b


def test_spool_filename_layout():
    obj = L._objects([H.frame(L, 0)])[0]
    name = L._spool_name(obj)
    assert re.fullmatch(r"disk\.\d{8}_\d{6}-\w+-[0-9a-f]{16}\.ndjson\.gz", name), name
    assert name.startswith("disk.20260824_120000-n2k-")


# ── object headers ───────────────────────────────────────────────────────

def test_data_objects_are_content_type_gzip_with_no_content_encoding():
    """ContentEncoding would tell HTTP-aware clients to transparently
    decompress, leaving plain ndjson on disk under a .gz name. The object IS
    the gzip archive, not a gzip-transported copy of something else. This
    convention has drifted once before."""
    seen = {}

    class Rec:
        def put_object(self, Bucket, Key, Body, **kw):
            seen.update(kw)

    put_object_gz(Rec(), "b", "raw/2026/08/24/120000-n2k-a.ndjson.gz", b"\x1f\x8b")
    assert seen.get("ContentType") == GZIP_CONTENT_TYPE == "application/gzip"
    assert "ContentEncoding" not in seen, "must NOT be set — see put_object_gz"


def test_log_objects_are_plain_uncompressed_json():
    """Deliberately unlike the data objects: the value of an operational log is
    being able to cat or grep it without a decompress step."""
    rec = _log_entry()
    assert rec["kw"].get("ContentType") == "application/json"
    assert "ContentEncoding" not in rec["kw"]
    assert not rec["body"].startswith(b"\x1f\x8b"), "not gzipped"
    json.loads(rec["body"])                        # parses as one JSON object
    assert b"\n  " in rec["body"], "indented for human reading"


# ── the candump line the exporter writes ─────────────────────────────────
#
# Not an archive format, but an interface all the same: canboat parses these,
# so the grammar is pinned here with everything else that outside tools read.

CANDUMP_LINE = re.compile(
    r"\A\((?P<epoch>\d+)\.(?P<usec>\d{6})\) (?P<iface>[A-Za-z0-9]+) "
    r"(?P<canid>[0-9A-F]{8})#(?P<data>([0-9A-F]{2}){0,8})\Z")


def _candump_row(**over):
    row = {"ts": "2017-08-17T14:12:12.106111+00:00", "mono": 1.0,
           "device_id": "sk", "src": "slcan0", "proto": "n2k",
           "raw": "09f50374#000a00ffff00ffff"}
    return {**row, **over}


def test_the_candump_line_is_the_documented_grammar():
    """The reference line, produced from a row. canboat reads these, so the
    spacing, the parenthesised timestamp and the case are the interface."""
    line = export.candump_line(_candump_row())
    assert line == "(1502979132.106111) slcan0 09F50374#000A00FFFF00FFFF"
    assert CANDUMP_LINE.fullmatch(line), line


def test_candump_upcases_the_hex_and_pads_the_identifier():
    """The archive stores lowercase and the identifier as written; the
    grammar is 8 uppercase hex digits, extended-frame width, always."""
    line = export.candump_line(_candump_row(raw="9f80102#a54dca182530bb1d"))
    assert CANDUMP_LINE.fullmatch(line).group("canid") == "09F80102"
    assert line.endswith("#A54DCA182530BB1D")


def test_candump_microseconds_are_always_six_digits():
    """`usec := 6DIGIT` — zero-padded, exactly six, whatever the value. A
    bare `%d` writes `.6` for six microseconds, which reads as 600000."""
    for usec in (0, 6, 106111, 999999):
        row = _candump_row(ts=f"2026-08-28T21:13:39.{usec:06d}+00:00")
        assert CANDUMP_LINE.fullmatch(export.candump_line(row)).group("usec") \
            == f"{usec:06d}"


def test_candump_round_trips_to_the_same_frame():
    """Re-casing and padding must not change what the line decodes to —
    otherwise canboat reads a different frame from the one captured."""
    row = _candump_row()
    m = CANDUMP_LINE.fullmatch(export.candump_line(row))
    assert (decode.n2k(f"{m.group('canid')}#{m.group('data')}")
            == decode.n2k(row["raw"]))


def test_candump_names_the_interface_but_never_invents_one():
    assert " slcan0 " in export.candump_line(_candump_row())
    # A row imported from a database that predates the field has no source,
    # and the grammar cannot say "unknown" — but `can0` would read as a real
    # capture from a real interface ever after.
    line = export.candump_line(_candump_row(src=None))
    assert CANDUMP_LINE.fullmatch(line).group("iface") == "unknown"


def test_candump_refuses_what_it_cannot_say():
    """Each of these would otherwise become a line canboat parses as a
    different frame. The exporter's per-object handler turns the exception
    into a skip and a message on stderr."""
    for bad in (_candump_row(proto="n0183", raw="$GPRMC,120000,A*6A"),
                # Well-formed as a frame, and still not one: a protocol this
                # build has never heard of has no CAN identifier either, and
                # the guard is on `proto`, not on whether the hex parses.
                _candump_row(proto="seatalk", raw="9c11#0011"),
                _candump_row(raw="09f50374"),                 # no separator
                _candump_row(raw="09f50374#000a0"),           # odd hex length
                _candump_row(raw="09f50374#00zz00ff"),        # not hex
                _candump_row(raw="09f50374#" + "aa" * 9),     # 9 data bytes
                _candump_row(ts="2017-08-17T14:12:12.106111")):   # naive
        try:
            export.candump_line(bad)
            assert False, f"should have been refused: {bad}"
        except ValueError:
            pass


def test_candump_writes_one_bare_line_per_row():
    """No header and no field list: unlike CSV, the line's shape comes from
    the format canboat reads, not from this archive's columns."""
    import io
    out = io.StringIO()
    write_row = export.make_writer(out, "candump", RAW_FIELDS)
    for i in range(3):
        write_row(_candump_row(ts=f"2017-08-17T14:12:1{i}.106111+00:00"))
    lines = out.getvalue().splitlines()
    assert len(lines) == 3, "a header would make this 4"
    assert all(CANDUMP_LINE.fullmatch(l) for l in lines)
    assert out.getvalue().endswith("\n")


def _run_exporter(argv: list[str]):
    """Drive main() with the network stubbed out, and report what it asked
    export_source for. argparse writes its usage to stderr on a rejected
    flag combination; swallowed here so the suite's own output stays
    readable — the exit code is what is being asserted."""
    import contextlib
    import io
    captured = {}

    def fake_export_source(s3, bucket, source, proto, since, until,
                            output_path, fmt, verbose):
        captured.update(source=source, proto=proto, fmt=fmt, output=output_path)
        return 0, 0, 0

    saved = (sys.argv, export.make_s3_client, export.export_source)
    sys.argv, export.make_s3_client, export.export_source = (
        ["nmea2s3-exporter", *argv], lambda *a, **kw: object(), fake_export_source)
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            export.main()
    finally:
        sys.argv, export.make_s3_client, export.export_source = saved
    return captured


def test_candump_narrows_to_n2k_before_anything_is_downloaded():
    """--proto filters on the object NAME, so implying it here means the
    objects a candump export cannot write are never fetched — rather than
    downloaded, decompressed and then dropped a row at a time."""
    assert _run_exporter(["--format", "candump"])["proto"] == "n2k"
    assert _run_exporter(["--format", "candump", "--proto", "n2k"])["proto"] == "n2k"
    # and the auto-named file says which protocol it holds
    assert str(_run_exporter(["--format", "candump", "-o"])["output"]).endswith("-n2k.candump")


def test_candump_refuses_the_exports_it_could_not_write():
    """Refused at the flags, before a LIST: an audit entry and an 0183
    sentence have no CAN frame in them at all."""
    for argv in (["--format", "candump", "--source", "_log"],
                 ["--format", "candump", "--proto", "n0183"]):
        try:
            _run_exporter(argv)
            assert False, f"should have been refused: {argv}"
        except SystemExit as e:
            assert e.code == 2, "argparse usage error"


# ── the data objects themselves ──────────────────────────────────────────

def test_a_written_object_is_gzipped_ndjson_one_row_per_line():
    lg = L.N2KLogger("can0")
    lg.s3 = H.FakeS3()
    import asyncio
    import tempfile
    lg.disk_dir = Path(tempfile.mkdtemp(prefix="n2ktest-fmt-"))
    for i in range(25):
        lg._append(H.frame(L, i))
    asyncio.run(lg._flush_buffer())
    asyncio.run(lg._upload_spool())

    key, body = next(iter(lg.s3.puts.items()))
    assert key.endswith(".ndjson.gz")
    assert body[:2] == b"\x1f\x8b", "gzip magic"
    text = gzip.decompress(body).decode("utf-8")
    lines = text.splitlines()
    assert len(lines) == 25
    assert text.endswith("\n")
    for line in lines:
        assert list(json.loads(line)) == RAW_FIELDS
    # ndjson, not a JSON array
    assert not text.lstrip().startswith("[")


def test_rows_within_an_object_are_ascending_in_ts():
    lg = L.N2KLogger("can0")
    lg.s3 = H.FakeS3()
    import asyncio
    import tempfile
    lg.disk_dir = Path(tempfile.mkdtemp(prefix="n2ktest-fmt-"))
    for i in range(50):
        lg._append(H.frame(L, i))
    asyncio.run(lg._flush_buffer())
    asyncio.run(lg._upload_spool())
    rows = lg.s3.rows()
    stamps = [r["ts"] for r in rows]
    assert stamps == sorted(stamps)
