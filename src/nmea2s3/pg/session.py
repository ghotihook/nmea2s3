"""The Postgres session nmea2s3-update-pg writes through: connected so a
dead link is noticed, locked so two runs never share a table, and able to
start over when the link dies under it.

A DEAD LINK IS NOTICED IN ABOUT A MINUTE, AT BOTH ENDS
------------------------------------------------------
A connection can die without either end being told: a laptop sleeps, a
router forgets a flow it thinks is idle, the Wi-Fi changes. Between two
objects the connection sits idle while the next one downloads and decodes,
and nothing notices until the next statement goes out and no reply comes
back. On 2026-09-14 that took minutes to become "Operation timed out", and
the run ended in a traceback.

TCP keepalives are how an idle connection notices. libpq turns them on but
leaves the timing to the OS, whose default is two hours before the first
probe — far longer than a router keeps an idle flow. So both ends are told:
probe after 30 s idle, every 10 s, give up after 3 unanswered. On the
client (libpq's `keepalives_*`) that makes the next statement fail at once
rather than hang, and the probes themselves stop a router deciding the flow
is idle. On the server (the per-session `tcp_keepalives_*` settings) it is
what ends the backend a dead client left behind, which is the backend
holding the lock below.

Keepalives cover an IDLE connection only. A statement already sent and
waiting for its reply is governed by TCP's retransmission timeout, which
nothing here can shorten on macOS (`tcp_user_timeout` is Linux-only). That
case is what the retry is for.

ONE RUN PER TABLE
-----------------
A session-level advisory lock keyed on the table name, taken on connecting.
A run that finds it held says who holds it and stops. Without it, a run
that hangs under a once-a-minute cron is joined by another every minute,
all fetching and writing the same keys — safe, since every write is an
upsert, but not cheap.

An advisory lock rather than a lock file because it belongs to the
connection: when the process dies, or its link does, the server lets go as
soon as it notices, which the keepalives above bound at about a minute. A
file lock is held by a hung process for as long as it hangs.

A session-level lock needs a real session. Through a pooler in transaction
mode (pgbouncer's default) the lock is held by whichever server connection
the pooler happened to use; point this at the server or a session-mode pool.

STARTING OVER, ONCE
-------------------
`call()` runs a unit of work and, if the connection breaks under it,
reconnects and runs it once more. Once: if a fresh connection fails
straight away the link is down, and the next run is the backoff. Only a
BROKEN connection is retried. An OperationalError the server sent, a full
disk say, arrives on a healthy connection and would fail the same way again.

The old backend may still be alive on the server when the new one connects,
not having noticed yet, and holding the table lock and whatever row locks
it was mid-write with. Waiting would mean waiting out its keepalives, so
the new session ends it instead. Only it: matched on pid AND backend_start,
so a pid the server has since reused names nobody.
"""

import hashlib
import logging
import time

import psycopg

APPLICATION = "nmea2s3-update-pg"

# Probe after 30 s idle, then every 10 s, and give up after 3 unanswered: a
# dead peer is noticed about a minute after the link went quiet.
KEEPALIVE_IDLE = 30
KEEPALIVE_INTERVAL = 10
KEEPALIVE_COUNT = 3

# How long a new session waits for the lock its terminated predecessor is
# releasing. pg_terminate_backend only signals; the backend exits a moment
# later.
LOCK_WAIT = 15.0
LOCK_POLL = 0.5

log = logging.getLogger("nmea2s3.pg")


class Busy(Exception):
    """Another session holds this table's lock."""


def lock_key(table: str) -> int:
    """The advisory lock id for `table`: 64 bits of a hash of this tool's
    name and the table's, so it collides with nothing else on the server."""
    digest = hashlib.blake2b(f"{APPLICATION}:{table}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


class Session:
    def __init__(self, config: dict, table: str, lock: bool = True):
        self.config = config
        self.table = table
        self.lock = lock
        self.con = None
        self.backend = None         # (pid, backend_start) of the current backend
        self._open()
        if lock:
            self._take_lock(wait=0)

    def _open(self) -> None:
        c = self.config
        self.con = psycopg.connect(
            host=c["pg_host"], port=c["pg_port"], dbname=c["pg_dbname"],
            user=c["pg_user"], password=c["pg_password"],
            connect_timeout=10, autocommit=True, application_name=APPLICATION,
            keepalives=1, keepalives_idle=KEEPALIVE_IDLE,
            keepalives_interval=KEEPALIVE_INTERVAL, keepalives_count=KEEPALIVE_COUNT)
        # set_config rather than `-c` in the startup options, which a pooler
        # may refuse to pass on.
        self.con.execute(
            "SELECT set_config('tcp_keepalives_idle', %s, false), "
            "set_config('tcp_keepalives_interval', %s, false), "
            "set_config('tcp_keepalives_count', %s, false)",
            (str(KEEPALIVE_IDLE), str(KEEPALIVE_INTERVAL), str(KEEPALIVE_COUNT)))
        self.backend = self.con.execute(
            "SELECT pid, backend_start FROM pg_stat_activity "
            "WHERE pid = pg_backend_pid()").fetchone()

    def _take_lock(self, wait: float) -> None:
        key = lock_key(self.table)
        deadline = time.monotonic() + wait
        while not self.con.execute("SELECT pg_try_advisory_lock(%s)", (key,)).fetchone()[0]:
            if time.monotonic() >= deadline:
                raise Busy(self._holder(key))
            time.sleep(LOCK_POLL)

    def _holder(self, key: int) -> str:
        # A bigint advisory lock shows in pg_locks as its high and low halves.
        row = self.con.execute(
            "SELECT a.pid, a.application_name, a.client_addr, a.backend_start "
            "FROM pg_locks l JOIN pg_stat_activity a USING (pid) "
            "WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1 "
            "AND a.datname = current_database() "
            "AND l.classid::bigint = %s AND l.objid::bigint = %s",
            ((key >> 32) & 0xFFFFFFFF, key & 0xFFFFFFFF)).fetchone()
        if row is None:
            return f"another session held {self.table} and has just let go"
        pid, app, addr, start = row
        return (f"{self.table} is held by pid {pid} ({app or 'unnamed'} from "
                f"{addr or 'a local socket'}, connected {start:%Y-%m-%d %H:%M:%S%z})")

    def call(self, what: str, fn, *args):
        """fn(con, *args), and once more on a new connection if this one
        breaks under it. `what` names the work in the warning."""
        try:
            return fn(self.con, *args)
        except psycopg.OperationalError as e:
            if not self.con.broken:
                raise
            log.warning("connection lost at %s (%s); reconnecting to retry it",
                        what, " / ".join(str(e).split("\n")))
        self.reconnect()
        return fn(self.con, *args)

    def reconnect(self) -> None:
        old = self.backend
        self.con.close()
        self._open()
        if old is not None:
            self.con.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE pid = %s AND backend_start = %s", old)
        if self.lock:
            self._take_lock(wait=LOCK_WAIT)

    def close(self) -> None:
        if self.con is not None:
            self.con.close()
