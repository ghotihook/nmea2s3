"""Shared fixtures for the test suite: fake S3, frame builders, audit capture.

Importing this module sets the environment the tools require and puts `src/`
on the path, so it MUST be imported before anything under `nmea2s3/`. The
environment part matters less than it used to — credentials are now read when
the logger is constructed rather than at import — but a test that builds an
N2KLogger still needs them present, and setting them here means no test has
to remember to.

Nothing here talks to a network. Every S3 interaction in the suite goes
through the fakes below.
"""

import gzip
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Run against the working tree, not against whatever version happens to be
# installed — `pip install -e .` would do the same, but the suite is meant to
# run on a boat with nothing set up but the logger's own dependencies.
sys.path.insert(0, str(REPO / "src"))

os.environ.setdefault("NMEA2S3_S3_ENDPOINT_URL", "http://fake.invalid")
os.environ.setdefault("NMEA2S3_S3_BUCKET", "test-bucket")
os.environ.setdefault("NMEA2S3_S3_ACCESS_KEY_ID", "test-key")
os.environ.setdefault("NMEA2S3_S3_SECRET_ACCESS_KEY", "test-secret")
os.environ.setdefault("NMEA2S3_DISK_DIR", tempfile.mkdtemp(prefix="n2ktest-spool-"))


# ── frames ───────────────────────────────────────────────────────────────

T0 = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)


# Sampled ONCE. Re-reading it per frame let microsecond jitter make otherwise
# identical frames differ, which silently hid a test that was reusing the same
# rows and expecting distinct content-addressed objects from them.
_EPOCH = time.time() - time.monotonic()


def epoch() -> float:
    """This host's boot epoch, so a built frame's two clocks agree. Constant
    for the life of the process, so identical inputs give identical frames."""
    return _EPOCH


def frame(logger_module, i: int, day_offset: int = 0, clock_skew: float = 0.0):
    """One Frame whose ts and mono are mutually consistent.

    `clock_skew` shifts the frame's apparent boot epoch, i.e. simulates it
    having been stamped by a clock that differs from the one running now —
    which is exactly what a mid-batch NTP step looks like in the data.
    """
    ts = T0 + timedelta(days=day_offset, milliseconds=i)
    mono = ts.timestamp() - (epoch() + clock_skew)
    return logger_module.build_frame(ts, mono, 0x09F80102, bytes([i % 256]) * 8)


# ── fake S3 ──────────────────────────────────────────────────────────────

class FakeS3:
    """Records what was PUT. `up=False` makes every call fail the way botocore
    does, so callers exercise their real error paths."""

    def __init__(self, up: bool = True):
        self.puts: dict[str, bytes] = {}
        self.headers: dict[str, dict] = {}
        self.up = up

    def _fail(self, op):
        from botocore.exceptions import ClientError
        raise ClientError({"Error": {"Code": "500"},
                           "ResponseMetadata": {"HTTPStatusCode": 500}}, op)

    def put_object(self, Bucket, Key, Body, **kw):
        if not self.up:
            self._fail("PutObject")
        self.puts[Key] = Body
        self.headers[Key] = kw          # ContentType etc, for format assertions

    def get_object(self, Bucket, Key):
        return {"Body": _Body(self.puts[Key])}

    # what the objects actually contain, for assertions
    def rows(self, key=None) -> list[dict]:
        keys = [key] if key else sorted(self.puts)
        out = []
        for k in keys:
            text = gzip.decompress(self.puts[k]).decode()
            out += [json.loads(line) for line in text.splitlines() if line]
        return out


class _Body:
    def __init__(self, b): self._b = b
    def read(self): return self._b


# ── audit-log capture ────────────────────────────────────────────────────

class AuditLog:
    """Intercepts nmea2s3.audit_log.log_action so a test can assert on which
    entries a tool writes — the point being that read-only work writes none."""

    def __init__(self):
        self.entries: list[dict] = []
        self._saved = None

    def __enter__(self):
        import nmea2s3.audit_log as al
        self._saved = al.log_action
        def capture(s3, bucket, application, exit_code, comment, details=None):
            self.entries.append({"application": application, "exit_code": exit_code,
                                 "comment": comment, **(details or {})})
            return "_log/fake.json"
        al.log_action = capture
        return self

    def __exit__(self, *a):
        import nmea2s3.audit_log as al
        al.log_action = self._saved
        return False
