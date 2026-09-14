"""nmea2s3-update-pg as a whole run: a connection that dies under it, a run
that cannot go on, and a table another run already has.

The failure reproduced here is real. On 2026-09-14 a catch-up run's
connection died while it was decoding the next object; the first statement
for that object, `CREATE TABLE IF NOT EXISTS`, waited minutes for a reply
that never came, and the run ended in a traceback.

Postgres is a fake that understands only the statements update.py and
session.py send: enough to keep a ledger, the columns, an advisory lock and
the backends holding one, and to break a connection on cue. S3 is
helpers.FakeS3 holding real capture objects, so the decode is the real one.
"""

import contextlib
import gzip
import io
import json
import os
import re
import sys
import unittest
from argparse import Namespace
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import helpers as H                                              # noqa: E402

try:
    import psycopg
    from botocore.exceptions import EndpointConnectionError
    from nmea2s3.pg import session, update
except ImportError as e:                                          # pragma: no cover
    raise unittest.SkipTest(f"decoder stack not importable: {e}")

TABLE = update.DEFAULT_TABLE
LEDGER = f"{TABLE}_objects"

CONFIG = {"s3_endpoint_url": "http://fake.invalid", "s3_bucket": "test-bucket",
          "s3_region": "us-east-1", "s3_access_key_id": "k", "s3_secret_access_key": "s",
          "pg_host": "db.invalid", "pg_port": 5432, "pg_dbname": "d",
          "pg_user": "u", "pg_password": "p"}

# What psycopg raised on 2026-09-14, verbatim.
LOST = ("consuming input failed: could not receive data from server: "
        "Operation timed out\nSSL SYSCALL error: Operation timed out")


def _nmea(body: str) -> str:
    checksum = 0
    for ch in body:
        checksum ^= ord(ch)
    return f"${body}*{checksum:02X}"


def _object(n: int) -> tuple[str, bytes]:
    """The n-th capture object: a minute of MWV at 1 Hz, five minutes after
    the one before."""
    start = H.T0 + timedelta(minutes=5 * n)
    lines = [json.dumps({"ts": (start + timedelta(seconds=i)).isoformat(), "mono": None,
                         "device_id": "boat-pi", "proto": "n0183", "src": None,
                         "raw": _nmea("IIMWV,045.0,R,10.5,N,A")})
             for i in range(60)]
    key = f"raw/{start:%Y/%m/%d}/{start:%H%M%S}-n0183-{n:016x}.ndjson.gz"
    return key, gzip.compress(("\n".join(lines) + "\n").encode())


class S3(H.FakeS3):
    """FakeS3 holding `objects` capture objects, remembering each GET."""

    def __init__(self, objects: int = 2):
        super().__init__()
        self.gets: list[str] = []
        self.keys: list[str] = []
        for n in range(objects):
            key, body = _object(n)
            self.puts[key] = body
            self.keys.append(key)

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        return super().get_object(Bucket, Key)


# ── a fake server ────────────────────────────────────────────────────────

class Result(list):
    def fetchone(self):
        return self[0] if self else None


class Pg:
    """One server. `connect` stands in for psycopg.connect. `fail(con, sql)`
    is consulted before every statement and may return an exception for the
    statement to raise."""

    def __init__(self, fail=None):
        self.fail = fail
        self.cons: list[Con] = []
        self.connects: list[dict] = []   # keyword arguments of each connect
        self.backends: dict = {}         # pid -> backend_start, alive server-side
        self.locks: dict = {}            # advisory key -> pid holding it
        self.terminated: list[int] = []
        self.ledger: set[str] = set()
        self.columns = {"ts"}
        self.upserts = 0

    def connect(self, **kw):
        self.connects.append(kw)
        con = Con(self, pid=100 + len(self.cons))
        self.cons.append(con)
        self.backends[con.pid] = con.start
        return con

    def end(self, pid):
        self.backends.pop(pid, None)
        self.locks = {k: p for k, p in self.locks.items() if p != pid}

    def held_elsewhere(self, table):
        """Another run's live session holds `table`."""
        self.backends[1] = H.T0
        self.locks[session.lock_key(table)] = 1


class Con:
    def __init__(self, server, pid):
        self.server, self.pid = server, pid
        self.start = H.T0 + timedelta(seconds=pid)
        self.broken = self.closed = False
        self.settings: dict = {}

    def drop(self):
        """The link dies. This end finds out; the server's backend, still
        holding whatever it held, does not."""
        self.broken = True
        return psycopg.OperationalError(LOST)

    def close(self):
        if not (self.broken or self.closed):
            self.server.end(self.pid)
        self.closed = True

    def execute(self, q, params=None):
        if self.broken or self.closed:
            raise psycopg.OperationalError("the connection is closed")
        sql = " ".join(q.split())
        error = self.server.fail and self.server.fail(self, sql)
        if error:
            raise error
        return Result(self._answer(sql, params))

    def _answer(self, sql, params):
        s = self.server
        if "set_config" in sql:
            names = re.findall(r"set_config\('(\w+)'", sql)
            self.settings.update(zip(names, map(int, params)))
            return [tuple(params)]
        if "WHERE pid = pg_backend_pid()" in sql:
            return [(self.pid, self.start)]
        if "pg_try_advisory_lock" in sql:
            holder = s.locks.get(params[0])
            if holder not in (None, self.pid) and holder in s.backends:
                return [(False,)]
            s.locks[params[0]] = self.pid
            return [(True,)]
        if "FROM pg_locks" in sql:
            return [(pid, "nmea2s3-update-pg", "192.0.2.7", s.backends[pid])
                    for pid in s.locks.values() if pid != self.pid][:1]
        if "pg_terminate_backend" in sql:
            pid, start = params
            if s.backends.get(pid) != start:
                return []
            s.terminated.append(pid)
            s.end(pid)
            return [(True,)]
        if sql.startswith(f"SELECT key FROM {LEDGER}"):
            return [(k,) for k in s.ledger]
        if "to_regclass" in sql:
            return [(c,) for c in s.columns]
        if "ADD COLUMN IF NOT EXISTS" in sql:
            s.columns.add(sql.split()[8])
        elif sql.startswith(f"INSERT INTO {LEDGER} "):
            s.ledger.add(params[0])
        elif sql.startswith(f"INSERT INTO {TABLE} "):
            s.upserts += 1
        return []

    def cursor(self):
        return self

    def copy(self, statement):
        class Copier:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def write(self, text): pass
        return Copier()


def _drop_at_second_object(s3, times: int):
    """Break the connection at the first statement written for the second
    object, once it has been downloaded — where it broke on 2026-09-14 —
    on `times` connections in a row."""
    dropped = []

    def fail(con, sql):
        if (len(dropped) < times and len(s3.gets) == 2
                and sql.startswith(f"CREATE TABLE IF NOT EXISTS {TABLE} (")):
            dropped.append(con)
            return con.drop()
    return fail


def _run(pg, s3, listing=None, **flags):
    """update.run against the fakes: (exit code, stderr, audit entries)."""
    args = Namespace(**{"bucket": "1s", "table": TABLE, "ledger": LEDGER, "proto": None,
                        "since": None, "until": None, "rebuild": False,
                        "dry_run": False, "verbose": False, **flags})
    saved = psycopg.connect, update.make_s3_client, update.iter_keys
    psycopg.connect = pg.connect
    update.make_s3_client = lambda *a: s3
    update.iter_keys = listing or (lambda *a: iter(s3.keys))
    err = io.StringIO()
    try:
        with H.AuditLog() as audit, contextlib.redirect_stderr(err):
            code = update.run(args, CONFIG)
    finally:
        psycopg.connect, update.make_s3_client, update.iter_keys = saved
    return code, err.getvalue(), audit.entries


# ── a connection that dies ───────────────────────────────────────────────

def test_a_connection_lost_between_objects_is_replaced_and_the_run_finishes():
    s3 = S3()
    pg = Pg()
    pg.fail = _drop_at_second_object(s3, times=1)
    code, err, _ = _run(pg, s3)
    assert code == 0, err
    assert pg.ledger == set(s3.keys), "both objects written and in the ledger"
    assert len(pg.cons) == 2
    assert s3.gets == s3.keys, \
        "the retry writes the rows already decoded; nothing is downloaded twice"


def test_the_new_session_ends_the_old_one_the_server_thinks_is_alive():
    """The server has not noticed the link die, so the old backend still
    holds the table lock. Waiting for its keepalives would stall the retry;
    ending it — that backend, matched on pid and start — does not."""
    s3 = S3()
    pg = Pg()
    pg.fail = _drop_at_second_object(s3, times=1)
    _run(pg, s3)
    assert pg.terminated == [pg.cons[0].pid]


def test_a_second_loss_stops_cleanly_and_says_how_to_resume():
    s3 = S3()
    pg = Pg()
    pg.fail = _drop_at_second_object(s3, times=2)
    code, err, _ = _run(pg, s3)
    assert code == update.EX_TEMPFAIL == 75
    assert len(pg.cons) == 2, "one retry, not a loop: the next run is the backoff"
    assert pg.ledger == {s3.keys[0]}, "the first object stays done, the second is not claimed"
    assert f"Stopped at {s3.keys[1]}" in err, err
    assert "Rerun" in err
    assert "Traceback" not in err


def test_columns_added_before_a_stop_still_reach_the_audit_log():
    """A schema change is one of the two things the audit log exists for, and
    a run that stops has made it just as surely as one that finishes."""
    s3 = S3()
    pg = Pg()
    pg.fail = _drop_at_second_object(s3, times=2)
    _, _, audit = _run(pg, s3)
    [entry] = audit
    assert entry["exit_code"] == update.EX_TEMPFAIL
    assert "mwv_wind_angle_r" in entry["new_columns"], entry


def test_an_error_the_server_sent_is_not_retried():
    """A full disk arrives on a healthy connection. A new one would meet the
    same disk."""
    def fail(con, sql):
        if sql.startswith(f"INSERT INTO {TABLE} "):
            return psycopg.errors.DiskFull("could not extend file")
    pg = Pg(fail)
    code, err, _ = _run(pg, S3())
    assert len(pg.cons) == 1, "a healthy connection is not replaced"
    assert code == update.EX_TEMPFAIL
    assert "could not extend file" in err, err
    assert pg.ledger == set()


def test_a_bug_still_ends_in_a_traceback():
    """A clean one-line stop is for conditions a rerun can fix. SQL this code
    got wrong is not one, and its traceback is what finds it."""
    def fail(con, sql):
        if sql.startswith(f"INSERT INTO {TABLE} "):
            return psycopg.errors.UndefinedColumn('column "x" does not exist')
    try:
        _run(Pg(fail), S3())
    except psycopg.errors.UndefinedColumn:
        pass
    else:
        assert False, "a programming error must not be turned into a clean stop"


def test_an_unreachable_bucket_stops_cleanly():
    def listing(*a):
        raise EndpointConnectionError(endpoint_url="http://fake.invalid")
    pg = Pg()
    code, err, _ = _run(pg, S3(), listing=listing)
    assert code == update.EX_TEMPFAIL
    assert "fake.invalid" in err, err
    assert "Traceback" not in err
    assert pg.upserts == 0


# ── one run per table ────────────────────────────────────────────────────

def test_a_table_another_run_holds_is_left_alone():
    pg = Pg()
    pg.held_elsewhere(TABLE)
    s3 = S3()
    code, err, _ = _run(pg, s3)
    assert code == 0, "under cron, another run already on it is the normal answer"
    assert s3.gets == [], "nothing downloaded"
    assert pg.upserts == 0 and pg.ledger == set()
    assert "held by pid 1" in err, err


def test_the_lock_is_per_table():
    """A 5 m table beside the 1 s one is a different run, not a competing one."""
    pg = Pg()
    pg.held_elsewhere(f"{TABLE}_5m")
    s3 = S3()
    code, err, _ = _run(pg, s3)
    assert code == 0, err
    assert pg.ledger == set(s3.keys)


def test_a_dry_run_takes_no_lock():
    """It writes nothing, so it competes with nothing."""
    pg = Pg()
    pg.held_elsewhere(TABLE)
    s3 = S3()
    code, err, _ = _run(pg, s3, dry_run=True)
    assert code == 0, err
    assert s3.gets == s3.keys
    assert pg.upserts == 0


# ── noticing a dead link ─────────────────────────────────────────────────

def test_a_dead_link_is_noticed_within_about_a_minute_at_both_ends():
    """Left to the OS, the first keepalive probe goes out after two hours
    idle. The client side is what makes the next statement fail at once
    rather than hang; the server side is what ends a dead run's backend, and
    with it the lock that turns every later run away."""
    pg = Pg()
    _run(pg, S3())
    kw, server = pg.connects[0], pg.cons[0].settings
    assert kw["keepalives"] == 1
    client = kw["keepalives_idle"] + kw["keepalives_interval"] * kw["keepalives_count"]
    far = (server["tcp_keepalives_idle"]
           + server["tcp_keepalives_interval"] * server["tcp_keepalives_count"])
    assert client <= 90, f"client notices after {client} s"
    assert far <= 90, f"server notices after {far} s"
