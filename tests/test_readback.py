"""Reading the archive back: ndjson.iter_rows_ndjson_gz.

The read path under nmea2s3-exporter, and under anything else that consumes the
archive. The memory shape matters because a day-aggregated object is several
GB decompressed while being under ~100MB on the wire.
"""

import gzip
import json
import os
import sys
import tracemalloc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import helpers as H                                              # noqa: E402
from nmea2s3.ndjson import gzip_and_id_stream, iter_rows_ndjson_gz   # noqa: E402

KEY = "n0183/2026/08/24/000000-deadbeef.ndjson.gz"


def _object(n):
    lines = [json.dumps({"ts": f"2026-08-24T00:00:{i % 60:02d}+00:00",
                         "device_id": "cmp135", "source_ip": "10.0.0.5",
                         "sentence_type": "RMC",
                         "raw_data": f"$GPRMC,{i:06d},A,3745.123,S,14512.456,E*6A"},
                        separators=(",", ":")) + "\n" for i in range(n)]
    _, body = gzip_and_id_stream(iter(lines))
    s3 = H.FakeS3()
    s3.puts[KEY] = body
    return s3, lines, body


def test_every_row_comes_back_intact():
    s3, lines, _ = _object(5000)
    got = list(iter_rows_ndjson_gz(s3, "test-bucket", KEY))
    assert len(got) == 5000
    assert got == [json.loads(l) for l in lines]
    assert list(got[0]) == ["ts", "device_id", "source_ip", "sentence_type", "raw_data"]


def test_peak_memory_is_the_compressed_size_not_the_decompressed_object():
    """The old form built one string of the whole decompressed object and then
    a list of every line in it — holding the object roughly twice over, so the
    migration could write day objects this could not read back."""
    s3, lines, body = _object(100_000)
    text_size = sum(len(l) for l in lines)
    tracemalloc.start()
    n = sum(1 for _ in iter_rows_ndjson_gz(s3, "test-bucket", KEY))
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert n == 100_000
    assert peak < text_size / 10, (
        f"peak {peak/1e6:.1f} MB against {text_size/1e6:.1f} MB of decompressed text")


def test_it_is_a_generator_not_a_materialised_list():
    """Callers rely on being able to stop early without paying for the rest."""
    s3, _, _ = _object(50_000)
    it = iter_rows_ndjson_gz(s3, "test-bucket", KEY)
    first = next(it)
    assert first["sentence_type"] == "RMC"
    it.close()


def test_a_corrupt_object_raises_rather_than_returning_nothing():
    """s3_export catches this at the call site to skip and continue; silently
    yielding zero rows would make a damaged object look like an empty one."""
    s3 = H.FakeS3()
    s3.puts[KEY] = b"this is not gzip"
    try:
        list(iter_rows_ndjson_gz(s3, "test-bucket", KEY))
        assert False, "a corrupt object must raise"
    except gzip.BadGzipFile:
        pass


def test_blank_lines_are_skipped():
    s3 = H.FakeS3()
    _, body = gzip_and_id_stream(iter(['{"ts":"2026-08-24T00:00:00+00:00"}\n', "\n",
                                       '{"ts":"2026-08-24T00:00:01+00:00"}\n']))
    s3.puts[KEY] = body
    assert len(list(iter_rows_ndjson_gz(s3, "test-bucket", KEY))) == 2
