"""The object format: RFC 1952 conformance, determinism, content addressing.

These objects are the archive. They are written once, kept forever, and read
by tools that do not exist yet, so the format is worth pinning down.
"""

import gzip
import hashlib
import io
import json
import os
import struct
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import helpers as H                                              # noqa: E402
from nmea2s3.ndjson import group_by_day, gzip_and_id_stream, s3_key   # noqa: E402


# The two properties gzip_and_id_stream has to hold, written out the obvious
# way: join the text, hash it, compress it. The shipped function does one
# streaming pass instead — it never materializes the joined text, because at
# the buffer cap that doubled the logger's peak RSS — and these are what say
# the fast shape still produces the identical id and the identical bytes.
# They live here rather than in the library precisely because nothing in
# production calls them; a checked-against oracle is their whole job.

def gzip_text(text: str) -> bytes:
    """mtime=0 is the point: gzip's header carries a compression timestamp
    and Python writes the current time into it by default, so identical rows
    produced different bytes and a different ETag every time."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(text.encode("utf-8"))
    return buf.getvalue()


def content_id(text: str) -> str:
    """Hashes the PRE-gzip text, never the gzip'd bytes — see gzip_text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _lines(n=5000):
    """The six-field record, and nothing else: `pgn`, `src_addr` and
    `priority` stopped being stored — they are pure functions of `raw` — but
    stayed in this fixture, sizing every object here against a row shape the
    logger cannot produce."""
    return [json.dumps({"ts": f"2026-08-24T00:0{i % 10}:00+00:00",
                        "mono": 58757.02 + i, "device_id": "signalk",
                        "src": "can0", "proto": "n2k",
                        "raw": f"09f8{i % 9999:04x}#aabbccddeeff0011"},
                       separators=(",", ":")) + "\n"
            for i in range(n)]


# ── RFC 1952 ─────────────────────────────────────────────────────────────

def test_header_is_a_conformant_gzip_member():
    body = gzip_text("".join(_lines(100)))
    id1, id2, cm, flg, mtime, xfl, os_byte = struct.unpack("<BBBBIBB", body[:10])
    assert (id1, id2) == (0x1F, 0x8B), "gzip magic"
    assert cm == 8, "CM=8 (deflate) is the only defined compression method"
    assert flg == 0, "no optional fields set"
    assert (flg >> 5) & 0x07 == 0, "reserved bits must be zero"
    assert mtime == 0, "MTIME=0 is what makes the bytes reproducible"


def test_trailer_crc_and_isize_are_correct():
    text = "".join(_lines(1000))
    body = gzip_text(text)
    crc, isize = struct.unpack("<II", body[-8:])
    raw = gzip.decompress(body)
    assert crc == zlib.crc32(raw) & 0xFFFFFFFF
    assert isize == len(raw) % 2 ** 32
    assert raw.decode() == text


def test_object_is_exactly_one_member_with_no_trailing_bytes():
    body = gzip_text("".join(_lines(100)))
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    d.decompress(body)
    assert d.unused_data == b"", "a reader that stops at the first member must get everything"


def test_payload_is_newline_terminated():
    """Objects are concatenable and line-oriented; a missing final newline
    would silently join two rows when files are catted together."""
    raw = gzip.decompress(gzip_text("".join(_lines(50)))).decode()
    assert raw.endswith("\n")


# ── determinism ──────────────────────────────────────────────────────────

def test_identical_content_produces_identical_bytes():
    """gzip stamps a compression timestamp by default, so the same rows used
    to yield different bytes and a different ETag every time."""
    text = "".join(_lines(2000))
    first = gzip_text(text)
    time.sleep(1.1)                       # long enough to move a 1s-resolution MTIME
    assert gzip_text(text) == first


def test_content_id_hashes_the_text_not_the_gzip_bytes():
    text = "".join(_lines(500))
    assert content_id(text) == content_id(text)
    assert content_id(text) != content_id(text + "x")


def test_streaming_and_joining_produce_the_same_object():
    """The logger streams; both must agree or the two writers would produce
    different bytes and different keys for the same rows."""
    lines = _lines(3000)
    text = "".join(lines)
    cid, body = gzip_and_id_stream(iter(lines))
    assert cid == content_id(text)
    assert body == gzip_text(text)


# ── keys ─────────────────────────────────────────────────────────────────

def test_key_layout_is_the_documented_one():
    assert (s3_key("n2k", "20260824", "120000", "abc123")
            == "raw/2026/08/24/120000-n2k-abc123.ndjson.gz")


def test_group_by_day_splits_on_the_utc_boundary():
    """The partition must reflect capture day, never upload day — a batch
    spanning midnight becomes two objects."""
    from nmea2s3 import logger as L
    from datetime import timedelta
    before = L.build_frame(H.T0.replace(hour=23, minute=59, second=59),
                           1.0, 0x09F80102, b"\x01" * 8)
    after = L.build_frame(H.T0.replace(hour=23, minute=59, second=59) + timedelta(seconds=2),
                          1.0, 0x09F80102, b"\x02" * 8)
    groups = group_by_day([before, after])
    assert [d for d, _ in groups] == ["20260824", "20260825"]
    assert all(len(g) == 1 for _, g in groups)
